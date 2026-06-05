extern crate bytes;
extern crate env_logger;
extern crate kcp;
extern crate rand;
extern crate time;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, Cursor, ErrorKind, Read, Write};
use std::rc::Rc;
use std::thread::sleep;
use std::time::Duration;

use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use kcp::Kcp;
use rand::Rng;

#[derive(Debug)]
struct DelayPacket {
    buf: BytesMut,
    ts: u32,
}

impl DelayPacket {
    fn new(buf: BytesMut) -> DelayPacket {
        DelayPacket { buf, ts: 0 }
    }

    fn len(&self) -> usize {
        self.buf.len()
    }

    fn ts(&self) -> u32 {
        self.ts
    }

    fn set_ts(&mut self, ts: u32) {
        self.ts = ts;
    }

    fn reader(self) -> Cursor<BytesMut> {
        Cursor::new(self.buf)
    }
}

struct Random {
    seeds: Vec<u32>,
    size: usize,
}

impl Random {
    fn new(size: usize) -> Random {
        Random {
            seeds: vec![0u32; size],
            size: 0,
        }
    }

    fn random(&mut self) -> u32 {
        if self.seeds.is_empty() {
            return 0;
        }

        if self.size == 0 {
            for (i, e) in self.seeds.iter_mut().enumerate() {
                *e = i as u32;
            }
            self.size = self.seeds.len();
        }

        let i = rand::rng().random_range(0..self.size);
        let x = self.seeds[i];

        self.size -= 1;
        self.seeds[i] = self.seeds[self.size];

        x
    }
}

#[inline]
fn current() -> u32 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1000000) as u32
}

struct LatencySimulator {
    lostrate: u32,
    rttmin: u32,
    rttmax: u32,
    nmax: usize,
    tx1: u32,
    tx2: u32,
    current: u32,
    p12: VecDeque<DelayPacket>,
    p21: VecDeque<DelayPacket>,
    r12: Random,
    r21: Random,
}

impl LatencySimulator {
    fn new(lostrate: u32, rttmin: u32, rttmax: u32, nmax: usize) -> LatencySimulator {
        LatencySimulator {
            lostrate: lostrate / 2,
            rttmin: rttmin / 2,
            rttmax: rttmax / 2,
            nmax,
            tx1: 0,
            tx2: 0,
            current: crate::current(),
            p12: VecDeque::new(),
            p21: VecDeque::new(),
            r12: Random::new(100),
            r21: Random::new(100),
        }
    }

    fn send(&mut self, peer: u32, data: &[u8]) -> usize {
        // println!("[VNET] SEND {} {:?}", peer, data);
        if peer == 0 {
            self.tx1 += 1;

            if self.r12.random() < self.lostrate {
                return data.len();
            }
            if self.p12.len() >= self.nmax {
                return data.len();
            }
        } else {
            self.tx2 += 1;

            if self.r21.random() < self.lostrate {
                return data.len();
            }
            if self.p21.len() >= self.nmax {
                return data.len();
            }
        }

        let mut pkg = DelayPacket::new(BytesMut::from(data));
        self.current = crate::current();

        let mut delay = self.rttmin;
        if self.rttmax > self.rttmin {
            delay += rand::random::<u32>() % (self.rttmax - self.rttmin);
        }

        pkg.set_ts(self.current + delay);

        if peer == 0 {
            self.p12.push_back(pkg);
        } else {
            self.p21.push_back(pkg);
        }

        data.len()
    }

    fn recv(&mut self, peer: u32, data: &mut [u8]) -> io::Result<usize> {
        {
            let pkg = if peer == 0 {
                match self.p12.front() {
                    None => {
                        return Err(io::Error::new(ErrorKind::WouldBlock, "No packet yet"));
                    }
                    Some(pkg) => pkg,
                }
            } else {
                match self.p21.front() {
                    None => {
                        return Err(io::Error::new(ErrorKind::WouldBlock, "No packet yet"));
                    }
                    Some(pkg) => pkg,
                }
            };

            self.current = crate::current();
            if self.current < pkg.ts() {
                return Err(io::Error::new(ErrorKind::WouldBlock, "No packet yet"));
            }

            if data.len() < pkg.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "Buffer is too small",
                ));
            }
        }

        let pkg = if peer == 0 {
            self.p12.pop_front().unwrap()
        } else {
            self.p21.pop_front().unwrap()
        };

        pkg.reader().read(data)
    }
}

struct KcpOutput {
    sim: Rc<RefCell<LatencySimulator>>,
    peer: u32,
}

impl Write for KcpOutput {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let mut sim = self.sim.borrow_mut();
        Ok(sim.send(self.peer, data))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
enum TestMode {
    Default,
    Normal,
    Fast,
}

fn run(mode: TestMode, msgcount: u32, lostrate: u32) {
    // Rtt 60ms ~ 125ms
    let vnet = LatencySimulator::new(lostrate, 60, 125, 1000);
    let vnet = Rc::new(RefCell::new(vnet));

    let mut kcp1 = Kcp::new(
        0x11223344,
        KcpOutput {
            sim: vnet.clone(),
            peer: 0,
        },
    );
    let mut kcp2 = Kcp::new(
        0x11223344,
        KcpOutput {
            sim: vnet.clone(),
            peer: 1,
        },
    );

    let mut current = crate::current();
    let mut slap = current + 20;
    let mut index = 0;
    let mut next = 0;
    let mut count = 0;
    let mut maxrtt = 0;

    // Set wnd size, average latency 200ms, 20ms per packet
    // Set max wnd to 128 considering packet lost and retry
    kcp1.set_wndsize(128, 128);
    kcp2.set_wndsize(128, 128);

    match mode {
        TestMode::Default => {
            kcp1.set_nodelay(0, 10, 0, false);
            kcp2.set_nodelay(0, 10, 0, false);
        }
        TestMode::Normal => {
            kcp1.set_nodelay(0, 10, 0, true);
            kcp2.set_nodelay(0, 10, 0, true);
        }
        TestMode::Fast => {
            kcp1.set_nodelay(2, 10, 2, true);
            kcp2.set_nodelay(2, 10, 2, true);

            kcp1.set_rx_minrto(10);
            kcp1.set_fast_resend(1);
        }
    }

    // let mut ts1 = ::current();

    let mut buf = [0u8; 2000];
    while next <= msgcount {
        sleep(Duration::from_millis(1));

        current = crate::current();
        kcp1.update(crate::current()).unwrap();
        kcp2.update(crate::current()).unwrap();

        // kcp1 send packet every 20ms
        while current >= slap {
            let mut buf = BytesMut::with_capacity(8);
            buf.put_u32_le(index);
            index += 1;
            buf.put_u32_le(current);

            kcp1.send(&buf).unwrap();
            // println!("SENT curr: {} {} {:?}", index, current, &buf[..]);

            slap += 20;
        }

        // vnet p1 -> p2
        loop {
            let mut vn = vnet.borrow_mut();
            match vn.recv(1, &mut buf) {
                Err(..) => break,
                Ok(n) => {
                    // println!("RECV kcp2 {:?}", &buf[..n]);
                    kcp2.input(&buf[..n]).unwrap();
                }
            }
        }

        // vnet p2 -> p1
        loop {
            let mut vn = vnet.borrow_mut();
            match vn.recv(0, &mut buf) {
                Err(..) => break,
                Ok(n) => {
                    // println!("RECV kcp1 {:?}", &buf[..n]);
                    kcp1.input(&buf[..n]).unwrap();
                }
            }
        }

        // kcp2 echos back
        loop {
            match kcp2.recv(&mut buf) {
                Err(..) => break,
                Ok(n) => {
                    // println!("ECHO kcp2 {:?}", &buf[..n]);
                    kcp2.send(&buf[..n]).unwrap();
                }
            }
        }

        // kcp1 checks response from kcp2
        loop {
            match kcp1.recv(&mut buf) {
                Err(..) => break,
                Ok(n) => {
                    let mut cur = Cursor::new(&buf[..n]);

                    let sn = cur.get_u32_le();
                    let ts = cur.get_u32_le();
                    // println!("[RECV] sn={} ts={}", sn, ts);
                    let rtt = current - ts;

                    if sn != next {
                        panic!(
                            "Received not continuously packet: sn {} <-> {}",
                            count, next
                        );
                    }

                    next += 1;
                    count += 1;

                    if rtt > maxrtt {
                        maxrtt = rtt;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kcp_default() {
        run(TestMode::Default, 1000, 10);
    }

    #[test]
    fn kcp_normal() {
        run(TestMode::Normal, 1000, 10);
    }

    #[test]
    fn kcp_fast() {
        run(TestMode::Fast, 1000, 10);
    }

    #[test]
    fn kcp_massive_lost_default() {
        run(TestMode::Default, 1000, 50);
    }

    #[test]
    fn kcp_massive_lost_normal() {
        run(TestMode::Normal, 1000, 50);
    }

    #[test]
    fn kcp_massive_lost_fast() {
        run(TestMode::Fast, 1000, 50);
    }

    // ─── Stream mode (T1.1) ───────────────────────────────────────────────

    fn run_stream(mode: TestMode, msgcount: u32, lostrate: u32) {
        let vnet = LatencySimulator::new(lostrate, 60, 125, 1000);
        let vnet = Rc::new(RefCell::new(vnet));

        let mut kcp1 = Kcp::new_stream(
            0x11223344,
            KcpOutput {
                sim: vnet.clone(),
                peer: 0,
            },
        );
        let mut kcp2 = Kcp::new(
            0x11223344,
            KcpOutput {
                sim: vnet.clone(),
                peer: 1,
            },
        );

        let mut current = crate::current();
        let mut slap = current + 20;
        let mut index = 0;
        let mut next = 0;
        let mut count = 0;
        let mut maxrtt = 0;

        kcp1.set_wndsize(128, 128);
        kcp2.set_wndsize(128, 128);

        match mode {
            TestMode::Default => {
                kcp1.set_nodelay(0, 10, 0, false);
                kcp2.set_nodelay(0, 10, 0, false);
            }
            TestMode::Normal => {
                kcp1.set_nodelay(0, 10, 0, true);
                kcp2.set_nodelay(0, 10, 0, true);
            }
            TestMode::Fast => {
                kcp1.set_nodelay(2, 10, 2, true);
                kcp2.set_nodelay(2, 10, 2, true);

                kcp1.set_rx_minrto(10);
                kcp1.set_fast_resend(1);
            }
        }

        assert!(kcp1.is_stream());

        let mut buf = [0u8; 2000];
        while next <= msgcount {
            sleep(Duration::from_millis(1));

            current = crate::current();
            kcp1.update(crate::current()).unwrap();
            kcp2.update(crate::current()).unwrap();

            while current >= slap {
                let mut buf = BytesMut::with_capacity(8);
                buf.put_u32_le(index);
                index += 1;
                buf.put_u32_le(current);

                kcp1.send(&buf).unwrap();

                slap += 20;
            }

            loop {
                let mut vn = vnet.borrow_mut();
                match vn.recv(1, &mut buf) {
                    Err(..) => break,
                    Ok(n) => {
                        kcp2.input(&buf[..n]).unwrap();
                    }
                }
            }

            loop {
                let mut vn = vnet.borrow_mut();
                match vn.recv(0, &mut buf) {
                    Err(..) => break,
                    Ok(n) => {
                        kcp1.input(&buf[..n]).unwrap();
                    }
                }
            }

            loop {
                match kcp2.recv(&mut buf) {
                    Err(..) => break,
                    Ok(n) => {
                        kcp2.send(&buf[..n]).unwrap();
                    }
                }
            }

            loop {
                match kcp1.recv(&mut buf) {
                    Err(..) => break,
                    Ok(n) => {
                        let mut cur = Cursor::new(&buf[..n]);

                        while cur.remaining() >= 8 {
                            let sn = cur.get_u32_le();
                            let ts = cur.get_u32_le();
                            let rtt = current - ts;

                            if sn != next {
                                panic!(
                                    "Received not continuously packet: recv_sn={} count={} next={}",
                                    sn, count, next
                                );
                            }

                            next += 1;
                            count += 1;

                            if rtt > maxrtt {
                                maxrtt = rtt;
                            }
                        }
                    }
                }
            }
        }
    }

    fn stream_append_test() {
        let mut kcp = Kcp::new_stream(0x11223344, io::Cursor::new(Vec::<u8>::new()));

        let mut buf1 = BytesMut::with_capacity(4);
        buf1.put_u32_le(0xDEAD);
        let n1 = kcp.send(&buf1).unwrap();
        assert_eq!(n1, 4);

        let mut buf2 = BytesMut::with_capacity(4);
        buf2.put_u32_le(0xBEEF);
        let n2 = kcp.send(&buf2).unwrap();
        assert_eq!(n2, 4);

        kcp.set_nodelay(0, 100, 0, true);
        kcp.update(1000).unwrap();

        assert_eq!(kcp.wait_snd(), 1);
    }

    // ─── T1.3 check() ─────────────────────────────────────────────────────

    #[test]
    fn kcp_check() {
        let mut kcp = Kcp::new(0x11223344, io::Cursor::new(Vec::new()));

        assert_eq!(kcp.check(1000), 1000);

        kcp.update(1000).unwrap();
        assert_eq!(kcp.check(1000), 1100);

        kcp.set_nodelay(2, 1000, 2, true);
        kcp.set_rx_minrto(10);
        kcp.set_fast_resend(1);

        let mut buf = BytesMut::with_capacity(8);
        buf.put_u32_le(0);
        buf.put_u32_le(0);
        kcp.send(&buf).unwrap();

        kcp.update(1200).unwrap();

        assert_eq!(kcp.check(1300), 1400);
        assert_eq!(kcp.check(1500), 1500);
        assert_eq!(kcp.check(1205), 1400);
    }

    #[test]
    fn kcp_check_not_updated() {
        let kcp = Kcp::new(0x11223344, io::Cursor::new(Vec::<u8>::new()));
        assert_eq!(kcp.check(u32::MAX), u32::MAX);
        assert_eq!(kcp.check(0), 0);
    }

    // ─── Stream tests (T1.1) ──────────────────────────────────────────────

    #[test]
    fn kcp_stream_default() {
        run_stream(TestMode::Default, 100, 10);
    }

    #[test]
    fn kcp_stream_normal() {
        run_stream(TestMode::Normal, 100, 10);
    }

    #[test]
    fn kcp_stream_fast() {
        run_stream(TestMode::Fast, 100, 10);
    }

    #[test]
    fn kcp_stream_append() {
        stream_append_test();
    }

    // ─── T1.2 Fragmentation ───────────────────────────────────────────────

    struct SharedBuf {
        buf: Rc<RefCell<Vec<u8>>>,
    }

    impl Write for SharedBuf {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.buf.borrow_mut().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn kcp_fragment_basic() {
        let out = Rc::new(RefCell::new(Vec::<u8>::new()));
        let mut kcp = Kcp::new(0x11223344, SharedBuf { buf: out.clone() });
        kcp.set_wndsize(128, 128);
        kcp.set_nodelay(2, 10, 2, true);
        kcp.set_rx_minrto(10);
        kcp.set_fast_resend(1);

        let payload: Vec<u8> = (0..2000).map(|i| i as u8).collect();
        kcp.send(&payload).unwrap();
        kcp.update(1000).unwrap();

        let wire = out.borrow().clone();
        assert!(!wire.is_empty());
        kcp.input(&wire).unwrap();

        let mut buf = [0u8; 4000];
        let n = kcp.recv(&mut buf).unwrap();
        assert_eq!(n, 2000);
        assert_eq!(&buf[..n], &payload);
    }

    #[test]
    fn kcp_fragment_large() {
        let out = Rc::new(RefCell::new(Vec::<u8>::new()));
        let mut kcp = Kcp::new(0x11223344, SharedBuf { buf: out.clone() });
        kcp.set_wndsize(128, 128);
        kcp.set_nodelay(2, 10, 2, true);
        kcp.set_rx_minrto(10);
        kcp.set_fast_resend(1);

        let payload: Vec<u8> = (0..5000).map(|i| i as u8).collect();
        kcp.send(&payload).unwrap();
        kcp.update(1000).unwrap();

        let wire = out.borrow().clone();
        assert!(!wire.is_empty());
        kcp.input(&wire).unwrap();

        let mut buf = [0u8; 6000];
        let n = kcp.recv(&mut buf).unwrap();
        assert_eq!(n, 5000);
        assert_eq!(&buf[..n], &payload);
    }

    // ─── T1.4 Window / Flow Control ───────────────────────────────────────

    macro_rules! pipe {
        ($src:ident, $dst:ident, $out:ident, $t:expr) => {{
            $src.update($t).unwrap();
            let data: Vec<u8> = $out.borrow_mut().drain(..).collect();
            if !data.is_empty() {
                $dst.input(&data).unwrap();
            }
        }};
    }

    #[test]
    fn kcp_flow_control_send_window() {
        // snd_wnd limits the number of segments in flight; sender must wait
        // for ACKs before sending more.
        let out1 = Rc::new(RefCell::new(Vec::<u8>::new()));
        let out2 = Rc::new(RefCell::new(Vec::<u8>::new()));

        let mut kcp1 = Kcp::new(0x11223344, SharedBuf { buf: out1.clone() });
        let mut kcp2 = Kcp::new(0x11223344, SharedBuf { buf: out2.clone() });

        // snd_wnd=3 so only 3 segments can be in flight
        // rcv_wnd is forced >= KCP_WND_RCV (128) so receiver is never a bottleneck
        kcp1.set_wndsize(3, 128);
        kcp2.set_wndsize(3, 128);
        kcp1.set_nodelay(0, 10, 0, true);

        // Send 5 segments — only 3 can be in flight (snd_wnd=3)
        for i in 0..5 {
            let mut msg = BytesMut::with_capacity(8);
            msg.put_u32_le(i);
            msg.put_u32_le(0);
            kcp1.send(&msg).unwrap();
        }
        assert_eq!(kcp1.wait_snd(), 5);

        // First flush: with cwnd=min(snd_wnd,rmt_wnd), only 3 segments move
        pipe!(kcp1, kcp2, out1, 1000);
        pipe!(kcp2, kcp1, out2, 1000);

        // kcp1 sent 3 (snd_wnd=3), 2 remain in snd_queue
        assert_eq!(kcp1.wait_snd(), 2, "2 segments should remain in snd_queue");

        // Drain 3 from kcp2 → ACKs tell kcp1 more space available
        let mut buf = [0u8; 2000];
        for _ in 0..3 {
            kcp2.recv(&mut buf).unwrap();
        }

        pipe!(kcp2, kcp1, out2, 1010);
        pipe!(kcp1, kcp2, out1, 1010);

        // Remaining 2 segments should now arrive at kcp2
        let mut received = 0;
        while let Ok(n) = kcp2.recv(&mut buf) {
            assert_eq!(n, 8);
            received += 1;
        }
        assert_eq!(received, 2, "both queued segments should arrive");
    }

    #[test]
    fn kcp_flow_control_blocked() {
        // snd_wnd=1: sender is blocked until ACK returns
        let out1 = Rc::new(RefCell::new(Vec::<u8>::new()));
        let out2 = Rc::new(RefCell::new(Vec::<u8>::new()));

        let mut kcp1 = Kcp::new(0x11223344, SharedBuf { buf: out1.clone() });
        let mut kcp2 = Kcp::new(0x11223344, SharedBuf { buf: out2.clone() });

        kcp1.set_wndsize(1, 128);
        kcp2.set_wndsize(1, 128);
        kcp1.set_nodelay(0, 10, 0, true);
        kcp2.set_nodelay(0, 10, 0, true);

        // Send 2 segments — 1st can be in flight (snd_wnd=1), 2nd stays in snd_queue
        let mut msg1 = BytesMut::with_capacity(8);
        msg1.put_u32_le(0);
        msg1.put_u32_le(0);
        kcp1.send(&msg1).unwrap();
        assert_eq!(kcp1.wait_snd(), 1);

        let mut msg2 = BytesMut::with_capacity(8);
        msg2.put_u32_le(1);
        msg2.put_u32_le(0);
        kcp1.send(&msg2).unwrap();
        assert_eq!(kcp1.wait_snd(), 2, "2nd segment should queue (snd_wnd=1)");

        // Move 1st to kcp2 — 1 in snd_buf (flight), 1 in snd_queue
        pipe!(kcp1, kcp2, out1, 1000);
        assert_eq!(kcp1.wait_snd(), 2, "1 in flight, 1 queued (snd_wnd=1)");

        // kcp2 acks
        pipe!(kcp2, kcp1, out2, 1000);

        // Now kcp1 can send the 2nd segment
        pipe!(kcp1, kcp2, out1, 1010);

        // kcp2 should have both
        let mut buf = [0u8; 2000];
        let mut received = 0;
        while let Ok(n) = kcp2.recv(&mut buf) {
            assert_eq!(n, 8);
            received += 1;
        }
        assert_eq!(received, 2, "both segments should arrive at kcp2");
    }
}
