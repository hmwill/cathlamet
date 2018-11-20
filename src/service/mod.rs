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

//! `service` is the http service end-point for a Cathlamet cluster.


use std::io::{Read, Write};
use std::net;

use bytes::BufMut;
use httparse::Status;
use may::net::TcpListener;

fn req_done(buf: &[u8], path: &mut String) -> Option<usize> {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = httparse::Request::new(&mut headers);

    if let Ok(Status::Complete(i)) = req.parse(buf) {
        path.clear();
        path.push_str(req.path.unwrap_or("/"));
        return Some(i);
    }

    None
}

pub struct HttpService {
    listener: TcpListener,
}

impl HttpService {
    pub fn new<A: net::ToSocketAddrs>(addr: A) -> HttpService {
        HttpService {
            listener: TcpListener::bind(addr).unwrap()
        }
    }

    pub fn run(&mut self) {
        while let Ok((mut stream, _)) = self.listener.accept() {
            go!(move || {
                let mut buf = Vec::new();
                let mut path = String::new();

                loop {
                    if let Some(i) = req_done(&buf, &mut path) {
                        let response = match &*path {
                            "/" => "Welcome to May http demo\n",
                            "/hello" => "Hello, World!\n",
                            "/quit" => std::process::exit(1),
                            _ => "Cannot find page\n",
                        };

                        let s = format!(
                            "\
                            HTTP/1.1 200 OK\r\n\
                            Server: May\r\n\
                            Content-Length: {}\r\n\
                            Date: 1-1-2000\r\n\
                            \r\n\
                            {}",
                            response.len(),
                            response
                        );

                        stream
                            .write_all(s.as_bytes())
                            .expect("Cannot write to socket");

                        buf = buf.split_off(i);
                    } else {
                        let mut temp_buf = vec![0; 512];
                        match stream.read(&mut temp_buf) {
                            Ok(0) => return, // connection was closed
                            Ok(n) => buf.put(&temp_buf[0..n]),
                            Err(err) => println!("err = {:?}", err),
                        }
                    }
                }
            });
        }
    }
}