//! How many relays each client address may still be given (`Config::relays_per_address`).

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Instant;

use crate::Rate;

/// Addresses tracked at once. Beyond that, an address not seen before is given no relay until
/// others have refilled, which keeps the table small under a flood.
const MAX_TRACKED: usize = 10_000;

pub struct Budgets {
    rate: Rate,
    buckets: HashMap<IpAddr, Bucket>,
}

#[derive(Clone, Copy)]
struct Bucket {
    tokens: f64,
    updated: Instant,
}

impl Bucket {
    fn refilled(self, rate: Rate, now: Instant) -> Bucket {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        Bucket {
            tokens: (self.tokens + elapsed * rate.per_second).min(rate.burst),
            updated: self.updated.max(now),
        }
    }
}

impl Budgets {
    pub fn new(rate: Rate) -> Self {
        Budgets {
            rate,
            buckets: HashMap::new(),
        }
    }

    /// Whether `ip` may be given one more relay, counting it if so.
    pub fn allow(&mut self, ip: IpAddr, now: Instant) -> bool {
        let rate = self.rate;
        if self.buckets.len() >= MAX_TRACKED && !self.buckets.contains_key(&ip) {
            self.buckets
                .retain(|_, bucket| bucket.refilled(rate, now).tokens < rate.burst);
            if self.buckets.len() >= MAX_TRACKED {
                return false;
            }
        }
        let fresh = Bucket {
            tokens: rate.burst,
            updated: now,
        };
        let bucket = self.buckets.entry(ip).or_insert(fresh);
        *bucket = bucket.refilled(rate, now);
        let allowed = bucket.tokens >= 1.0;
        if allowed {
            bucket.tokens -= 1.0;
        }
        allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const RATE: Rate = Rate {
        per_second: 0.1,
        burst: 3.0,
    };

    #[test]
    fn an_address_gets_its_burst_then_one_every_ten_seconds() {
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let mut budgets = Budgets::new(RATE);
        for _ in 0..3 {
            assert!(budgets.allow(ip, start));
        }
        assert!(!budgets.allow(ip, at(9)));
        assert!(budgets.allow(ip, at(10)));
        assert!(!budgets.allow(ip, at(10)));
        assert!(
            budgets.allow("203.0.113.10".parse().unwrap(), at(10)),
            "its own budget"
        );
        assert!(budgets.allow(ip, at(1000)) && budgets.allow(ip, at(1000)));
        assert!(
            budgets.allow(ip, at(1000)) && !budgets.allow(ip, at(1000)),
            "at most 3"
        );
    }

    #[test]
    fn a_full_table_forgets_refilled_addresses_before_refusing_new_ones() {
        let start = Instant::now();
        let mut budgets = Budgets::new(RATE);
        for n in 0..MAX_TRACKED as u32 {
            budgets.allow(IpAddr::from(n.to_be_bytes()), start);
        }
        let newcomer: IpAddr = "198.51.100.1".parse().unwrap();
        assert!(!budgets.allow(newcomer, start), "nothing has refilled");
        assert!(budgets.allow(newcomer, start + Duration::from_secs(10)));
        assert!(budgets.buckets.len() < MAX_TRACKED);
    }
}
