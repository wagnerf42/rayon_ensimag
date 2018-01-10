use rayon;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use super::increment::parallel_increment;

/// Compute a prefix algorithm with given function on given mutable slice.
/// # Example
/// ```
/// use rayon_ensimag::prelude::*;
///
/// let mut v:Vec<_> = (0..10_000).into_iter().collect();
/// let s = v.as_mut_slice().prefix(|a, b| a + b);
/// for (i, e) in v.iter().enumerate() {
///     assert_eq!(i*(i+1)/2, *e)
/// }
/// ```
pub trait Prefixable<T: Send + Sync + Copy, F: Fn(&T, &T) -> T> {
    fn prefix(&mut self, op: F);
}

impl<'a, T: Send + Sync + Copy, F: Fn(&T, &T) -> T + Sync> Prefixable<T, F> for &'a mut [T] {
    fn prefix(&mut self, op: F) {
        let length = self.len() as f64;
        adaptive_prefix(
            self,
            &op,
            length.sqrt().ceil() as usize,
            length.ln().ceil() as usize,
        );
    }
}

pub fn sequential_prefix<T, F: Fn(&T, &T) -> T>(slice: &mut [T], op: &F) {
    for i in 1..slice.len() {
        slice[i] = op(&slice[i], &slice[i - 1]);
    }
}

pub fn adaptive_prefix<T: Copy + Send + Sync, F: Fn(&T, &T) -> T + Sync>(
    slice: &mut [T],
    op: &F,
    macro_blocks_size: usize,
    micro_blocks_size: usize,
) {
    let mut last_value = None;
    for macro_slice in slice.chunks_mut(macro_blocks_size) {
        if let Some(ref value) = last_value {
            macro_slice[0] = op(&macro_slice[0], value);
        }
        parallel_prefix(macro_slice, op, micro_blocks_size);
        last_value = macro_slice.last().cloned();
    }
}

pub fn parallel_prefix<T: Copy + Send + Sync, F: Fn(&T, &T) -> T + Sync>(
    slice: &mut [T],
    op: &F,
    micro_blocks_size: usize,
) {
    let stolen = &AtomicBool::new(false);
    let finished = &AtomicBool::new(false);

    let (tx, rx) = channel();
    let (final_slice_tx, final_slice_rx) = channel();
    let (final_value_tx, final_value_rx) = channel();
    rayon::join(
        move || {
            let mut last_value = None;
            let achieved = slice
                .chunks_mut(micro_blocks_size)
                .take_while(|_| !stolen.load(Ordering::Relaxed))
                .map(|s| {
                    if let Some(ref value) = last_value {
                        s[0] = op(&s[0], value);
                    }
                    sequential_prefix(s, op);
                    last_value = Some(*s.last().unwrap())
                })
                .count();
            let next_start = achieved * micro_blocks_size;
            if next_start >= slice.len() {
                tx.send(None).expect("sending no work failed");
            } else {
                let (_, remaining) = slice.split_at_mut(next_start);
                if let Some(ref value) = last_value {
                    remaining[0] = op(&remaining[0], value);
                }
                if remaining.len() < micro_blocks_size {
                    // low work remaining. do it ourselves
                    tx.send(None).expect("sending no work failed");
                    sequential_prefix(remaining, op);
                } else {
                    let half = remaining.len() / 2;
                    let (my_work, given_work) = remaining.split_at_mut(half);
                    tx.send(Some(given_work)).expect("sending work failed");
                    parallel_prefix(my_work, op, micro_blocks_size);
                    let final_value = *my_work.last().unwrap();
                    if finished.swap(true, Ordering::Relaxed) {
                        let slice_to_increment = final_slice_rx
                            .recv()
                            .expect("failed receiving prefix slice to increment");
                        parallel_increment(
                            slice_to_increment,
                            &|v| op(v, &final_value),
                            micro_blocks_size,
                        );
                    } else {
                        final_value_tx
                            .send(final_value)
                            .expect("sending final value failed");
                    }
                }
            }
        },
        move || {
            steal_prefix_task(
                op,
                &rx,
                &final_slice_tx,
                &final_value_rx,
                stolen,
                finished,
                micro_blocks_size,
            )
        },
    );
}

fn steal_prefix_task<'a, T: Copy + Send + Sync, F: Fn(&T, &T) -> T + Sync>(
    op: &F,
    rx: &Receiver<Option<&'a mut [T]>>,
    slice_tx: &Sender<&'a mut [T]>,
    value_rx: &Receiver<T>,
    stealing: &AtomicBool,
    finished: &AtomicBool,
    micro_blocks_size: usize,
) {
    stealing.store(true, Ordering::Relaxed);
    if let Some(slice) = rx.recv().expect("receiving prefix failed") {
        parallel_prefix(slice, op, micro_blocks_size);
        if finished.swap(true, Ordering::Relaxed) {
            let final_value = value_rx.recv().expect("failed receiving final value");
            parallel_increment(slice, &|v| op(v, &final_value), micro_blocks_size);
        } else {
            slice_tx
                .send(slice)
                .expect("sending last prefix slice failed");
        }
    }
}
