//! Order #15 — maker-book paper bot (Part A) and up/down universe recorder (Part B).
//!
//! This crate is INTENTIONALLY separate from `rust_bot` (the taker bot). It has its
//! own `Cargo.toml`, its own `target/`, its own config and its own log directory, and
//! it shares no mutable state with the taker process. A panic, memory leak or WS
//! storm on the maker side therefore cannot disturb the taker bot or the running
//! audition (order A6).

pub mod bnws;
pub mod fill_model;
pub mod health;
pub mod jsonl;
pub mod pmws;
pub mod universe;
