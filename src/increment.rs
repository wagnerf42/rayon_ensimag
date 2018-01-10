
use rayon;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;

pub fn parallel_increment<T: Send, F: Fn(&T) -> T + Sync>(
    slice: &mut [T],
    op: &F,
    micro_blocks_size: usize,
) {
    let stolen = &AtomicBool::new(false);
    let stealing = stolen.clone();
    let (tx, rx) = channel();

    rayon::join(
        move || {
            let achieved = slice
                .chunks_mut(micro_blocks_size)
                .take_while(|_| !stolen.load(Ordering::Relaxed))
                .map(|s| sequential_increment(s, op))
                .count();
            let next_start = achieved * micro_blocks_size;
            if next_start >= slice.len() {
                tx.send(None).expect("sending no work failed");
            } else {
                let (_, remaining) = slice.split_at_mut(next_start);
                let half = remaining.len() / 2;
                let (my_work, given_work) = remaining.split_at_mut(half);
                tx.send(Some(given_work)).expect("sending work failed");
                parallel_increment(my_work, op, micro_blocks_size);
            }
        },
        move || {
            stealing.store(true, Ordering::Relaxed);
            rx.recv()
                .expect("receiving work failed")
                .map(|s| parallel_increment(s, op, micro_blocks_size))
        },
    );
}

pub fn sequential_increment<T, F: Fn(&T) -> T>(slice: &mut [T], op: &F) {
    for e in slice {
        *e = op(e);
    }
}
