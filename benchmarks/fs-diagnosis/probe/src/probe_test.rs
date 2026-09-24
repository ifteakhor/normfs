use super::*;

#[test]
fn counters_cover_workers_and_reset_between_windows() {
    start(true);
    let workers: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                for _ in 0..100 {
                    let _timer = Timer::new(Metric::Write);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(stop_json().contains("\"write\":{\"calls\":400,"));
    start(false);
    drop(Timer::new(Metric::Write));
    assert!(stop_json().contains("\"write\":{\"calls\":0,\"seconds\":0}"));
}
