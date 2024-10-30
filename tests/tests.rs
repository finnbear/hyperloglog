use hyperloglog::HyperLogLog;

fn test_accuracy<T: HyperLogLog>(mut hll: T) -> f64 {
    let mut count = 1;
    let mut max_error = f64::NEG_INFINITY;
    let mut max_error_count = 0;
    while count < 1000000 {
        const SAMPLES: usize = 64;

        for _ in 0..SAMPLES {
            hll.clear();
            for _ in 0..count {
                hll.insert(&rand::random::<u128>());
            }
            let estimate = hll.estimate();
            let error = (estimate - count as f64).abs() / count as f64;
            if error > max_error {
                max_error = error;
                max_error_count = count;
            }
        }

        count *= 10;
    }
    println!(
        "with {}, {max_error:.3} (at {max_error_count:>6})",
        T::PRECISION
    );
    max_error
}

#[test]
fn test_accuracies() {
    println!("u8");
    test_accuracy([0u8; 16]);
    test_accuracy([0u8; 32]);
    test_accuracy([0u8; 64]);
    test_accuracy([0u8; 128]);
    test_accuracy([0u8; 256]);
    println!();

    println!("u32");
    test_accuracy([0u32; 4]);
    test_accuracy([0u32; 8]);
    test_accuracy([0u32; 16]);
    test_accuracy([0u32; 32]);
    test_accuracy([0u32; 64]);
    println!();

    println!("u64");
    test_accuracy([0u64; 2]);
    test_accuracy([0u64; 4]);
    test_accuracy([0u64; 8]);
    test_accuracy([0u64; 16]);
    test_accuracy([0u64; 32]);
    println!();

    println!("u128");
    test_accuracy(0u128);
    test_accuracy([0u128; 2]);
    test_accuracy([0u128; 4]);
    test_accuracy([0u128; 8]);
    test_accuracy([0u128; 16]);
    test_accuracy([0u128; 32]);
    test_accuracy([0u128; 64]);
    println!();
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
