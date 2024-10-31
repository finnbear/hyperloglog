use hyperloglog::{HyperLogLog, Registers};

fn test_accuracy<R: Registers>() -> f64 {
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
            for _ in 0..count {
                hll.insert(&rand::random::<u128>());
            }
            let estimate = hll.estimate();

            let json = serde_json::to_string(&hll).unwrap();
            let unjson = serde_json::from_str::<HyperLogLog<R>>(&json).unwrap();
            assert!(hll == unjson);

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
fn test_accuracies() {
    test_accuracy::<[u8; 16]>();
    test_accuracy::<[u8; 32]>();
    test_accuracy::<[u8; 64]>();
    test_accuracy::<[u8; 128]>();
    test_accuracy::<[u8; 256]>();
    test_accuracy::<[u8; 512]>();
    test_accuracy::<[u8; 1024]>();
    test_accuracy::<[u8; 2048]>();
}

#[test]
fn hyperloglog_test_simple() {
    let mut hll = [0u8; 16];
    let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
    for k in &keys {
        hll.insert(k);
    }
    assert!((hll.estimate().round() - 3.0).abs() < std::f64::EPSILON);
    hll.clear();
    assert!(hll.estimate() == 0.0);
}

#[test]
fn hyperloglog_test_merge() {
    let mut hll = [0u8; 64];
    let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
    for k in &keys {
        hll.insert(k);
    }
    assert!((hll.estimate().round() - 3.0).abs() < std::f64::EPSILON);

    let mut hll2 = [0u8; 64];
    let keys2 = ["test3", "test4", "test4", "test4", "test4", "test1"];
    for k in &keys2 {
        hll2.insert(k);
    }
    assert!(
        (hll2.estimate().round() - 3.0).abs() < std::f64::EPSILON,
        "{}",
        hll2.estimate().round()
    );

    hll.merge(&hll2);
    assert!((hll.estimate().round() - 4.0).abs() < std::f64::EPSILON);
}
