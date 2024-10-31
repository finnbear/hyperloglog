#![no_main]
use libfuzzer_sys::fuzz_target;
use hyperloglog::HyperLogLog;

fuzz_target!(|data: &[u8]| {
    if let Ok(hll) = serde_json::from_slice::<HyperLogLog<[u8; 16]>>(data) {
        let encode = serde_json::to_string(&hll).unwrap();
        let decode = serde_json::from_str::<HyperLogLog<[u8; 16]>>(&encode).unwrap();
        assert_eq!(hll, decode);

        let _ = hll.cardinality();
    }

    let mut hll = HyperLogLog::<[u8; 16]>::default();
    for b in data {
        hll.insert(b);
    }
    serde_json::to_string(&hll).unwrap();
    let _ = hll.cardinality();
});
