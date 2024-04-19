use std::path::Path;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use store::Store;

fn insert_records(store: &mut Store, keys: &[Vec<u8>]) {
    for key in keys.iter() {
        store.put(key, key);
    }
}

fn insert_1_000_000_fit_in_memory(c: &mut Criterion) {
    let mut store = Store::new(
        Path::new("tmp_bench_files/insert_1_000_000_will_fit_in_memory"),
        false,
    );
    store.mem_table_size_limit_in_bytes = 1024 * 1024 * 100;
    let n = 1_000_000;
    let keys: Vec<Vec<u8>> = (0_u32..n)
        .into_iter()
        .map(|v| (v + 5000000).to_le_bytes().to_vec())
        .collect();

    c.bench_function("1000000 records, all fit in memory", |b| {
        b.iter(|| insert_records(black_box(&mut store), black_box(&keys)))
    });
}

fn insert_1_000_000_wont_fit_in_memory(c: &mut Criterion) {
    let mut store = Store::new(
        Path::new("tmp_bench_files/insert_1_000_000_wont_fit_in_memory"),
        false,
    );
    store.mem_table_size_limit_in_bytes = 1024 * 1024 * 4;
    let n = 1_000_000;
    let keys: Vec<Vec<u8>> = (0_u32..n)
        .into_iter()
        .map(|v| (v).to_le_bytes().to_vec())
        .collect();

    c.bench_function("1000000 records, wont all fit in memory", |b| {
        b.iter(|| insert_records(black_box(&mut store), black_box(&keys)))
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = insert_1_000_000_fit_in_memory, insert_1_000_000_wont_fit_in_memory
);
criterion_main!(benches);
