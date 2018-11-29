// MIT License
//
// Copyright (c) 2018 Hans-Martin Will
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! # Router
//!
//! `router` is the main program for a request router node process within a cathlamet cluster.

#![feature(await_macro, async_await, futures_api)]

extern crate cathlamet;
extern crate clap;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;
extern crate may;
extern crate nix;
extern crate pretty_env_logger;

use clap::{App, Arg, SubCommand};

use std::sync::atomic;

use nix::sys::signal;

lazy_static! {
    static ref termination_flag: atomic::AtomicBool = atomic::AtomicBool::from(false);
}

fn main() {
    pretty_env_logger::init();

    let matches = App::new("Router")
        .version(cathlamet::VERSION)
        .author(cathlamet::AUTHOR)
        .about(cathlamet::DESCRIPTION)
        .get_matches();
    info!("Cathlamet Request Router Node");

    may::config().set_io_workers(4);

    let address = "127.0.0.1:8080";
    let mut server = cathlamet::service::HttpService::new(address, &termination_flag);
    info!("Starting HTTP service listening at {}", address);
    server.run();
}
