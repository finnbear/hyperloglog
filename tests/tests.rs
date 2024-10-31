use hyperloglog::{HyperLogLog, Registers};

fn test_precision<R: Registers>() -> f64 {
    let mut hll = HyperLogLog::<R>::default();
    let mut count = 1;
    let mut max_error = f64::NEG_INFINITY;
    let mut max_error_count = 0;
    let mut min_compressed_size = usize::MAX;
    let mut max_compressed_size = 0;
    while count < 10000000 {
        const SAMPLES: usize = 64;

        for _ in 0..SAMPLES {
            hll.clear();
            for i in 0..count {
                if i < 10 {
                    let json = serde_json::to_string(&hll).unwrap();
                    let unjson = serde_json::from_str::<HyperLogLog<R>>(&json).unwrap();
                    assert!(hll == unjson);
                }
                hll.insert(&rand::random::<u128>());
            }
            let estimate = hll.estimate();

            let compressed = bincode::serialize(&hll).unwrap();
            min_compressed_size = min_compressed_size.min(compressed.len() - 8);
            max_compressed_size = max_compressed_size.max(compressed.len() - 8);
            let decompressed = bincode::deserialize::<HyperLogLog<R>>(&compressed).unwrap();
            assert!(hll == decompressed);
            let error = (estimate as f64 - count as f64).abs() / count as f64;
            if error > max_error {
                max_error = error;
                max_error_count = count;
            }
        }

        count *= 10;
    }
    println!(
        "with {}, {max_error:.3} (at {max_error_count:>6}), size {:.2} - {:.2}",
        R::PRECISION,
        min_compressed_size as f32 / R::REGISTERS as f32,
        max_compressed_size as f32 / R::REGISTERS as f32
    );
    max_error
}

#[test]
fn test_precisions() {
    test_precision::<[u8; 16]>();
    test_precision::<[u8; 32]>();
    test_precision::<[u8; 64]>();
    test_precision::<[u8; 128]>();
    test_precision::<[u8; 256]>();
    test_precision::<[u8; 512]>();
    test_precision::<[u8; 1024]>();
    test_precision::<[u8; 2048]>();
}

#[test]
fn hyperloglog_test_simple() {
    let mut hll = HyperLogLog::<[u8; 64]>::default();
    let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
    for k in &keys {
        hll.insert(k);
    }
    assert_eq!(hll.estimate(), 3);
    hll.clear();
    assert_eq!(hll.estimate(), 0);
}

#[test]
fn hyperloglog_test_merge() {
    let mut hll = HyperLogLog::<[u8; 64]>::default();
    let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
    for k in &keys {
        hll.insert(k);
    }
    assert_eq!(hll.estimate(), 3);

    let mut hll2 = HyperLogLog::<[u8; 64]>::default();
    let keys2 = ["test3", "test4", "test4", "test4", "test4", "test1"];
    for k in &keys2 {
        hll2.insert(k);
    }
    assert_eq!(hll2.estimate(), 3);

    hll.merge(&hll2);
    assert_eq!(hll.estimate(), 4);
}
