extern crate rayon_ensimag;
use rayon_ensimag::prelude::*;
use std::iter::repeat;

fn main() {
    let mut v: Vec<_> = repeat(1).take(1_000_000).collect();
    v.as_mut_slice().prefix(|a, b| a + b);
    for (x1, x2) in v.iter().zip((1..1_000_001).into_iter()) {
        assert_eq!(*x1, x2);
    }
}
