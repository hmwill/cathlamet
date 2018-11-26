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


use std::io;
use std::net;
use std::str;
use std::sync::atomic;

use may::net::TcpListener;

use http_muncher::{Parser, ParserHandler};

struct InsertHandler {}

impl ParserHandler for InsertHandler {

}

struct UpdateHandler {}

impl ParserHandler for UpdateHandler {

}

struct DeleteHandler {}

impl ParserHandler for DeleteHandler {

}

// Now let's define a new listener for parser events:
struct RequestHandler {
    is_complete: bool
}

impl RequestHandler {
    fn new() -> RequestHandler {
        RequestHandler {
            is_complete: false
        }
    }
}

impl ParserHandler for RequestHandler {
    fn on_url(&mut self, _parser: &mut Parser, url: &[u8]) -> bool { 
        trace!("URL = {}: ", str::from_utf8(url).unwrap());
        true
    }

    fn on_header_field(&mut self, _parser: &mut Parser, header: &[u8]) -> bool {
        // Print the received header key part
        trace!("{}: ", str::from_utf8(header).unwrap());

        // We have nothing to say to parser, and we'd like
        // it to continue its work - so let's return "true".
        true
    }

    fn on_header_value(&mut self, _parser: &mut Parser, value: &[u8]) -> bool {
        trace!("\t{}", str::from_utf8(value).unwrap());
        true
    }

    fn on_body(&mut self, _parser: &mut Parser, text: &[u8]) -> bool {  
        trace!("{}", str::from_utf8(text).unwrap());
        true
    }

    fn on_headers_complete(&mut self, _parser: &mut Parser) -> bool {  
        trace!("Headers complete");
        true
    }

    fn on_message_begin(&mut self, _parser: &mut Parser) -> bool {  
        trace!("Message begin");
        true
    }

    fn on_message_complete(&mut self, _parser: &mut Parser) -> bool { 
        trace!("Message complete");
        self.is_complete = true;
        true
    }

    fn on_chunk_header(&mut self, _parser: &mut Parser) -> bool { 
        trace!("Chunk header");
        true
    }

    fn on_chunk_complete(&mut self, _parser: &mut Parser) -> bool { 
        trace!("Chunk complete");
        true
    }
}

fn process_request(stream: may::net::TcpStream) {
    use std::io::{BufRead, Write};

    info!("Processing new request");

    let mut callback_handler = RequestHandler::new();
    let mut parser = Parser::request();

    let mut stream = {
        //stream.set_nonblocking(true).unwrap();
        let mut buf_reader = io::BufReader::new(stream);

        while !callback_handler.is_complete {
            match buf_reader.fill_buf() {
                Ok(buffer) => {
                    info!("Filled a buffer with {} elements", buffer.len());
                    let consumed = parser.parse(&mut callback_handler, buffer);

                    if buffer.len() == 0 {
                        // we check for an empty buffer down here, because the parser engine needs to be
                        // informed about the end of the message prior to leaving the loop.
                        break;
                    }

                    buf_reader.consume(consumed);
                },
                Err(err) => {
                    error!("I/O error while reading request data: {}", err);
                    break;
                }
            }
        }
        buf_reader.into_inner()
    };

    let response = "Hello, World!\r\n";
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
    }

pub struct HttpService<'a> {
    listener: TcpListener,
    stop: &'a atomic::AtomicBool
}

impl <'a> HttpService<'a> {
    pub fn new<A: net::ToSocketAddrs>(addr: A, stop: &'a atomic::AtomicBool) -> HttpService {
        HttpService {
            listener: TcpListener::bind(addr).unwrap(),
            stop
        }
    }

    pub fn run(&mut self) {
        while let Ok((stream, _)) = self.listener.accept() {
            go!(move || {
                process_request(stream)
            });

            if self.stop.load(atomic::Ordering::Relaxed) {
                break
            }
        }
    }

    pub fn stop(&mut self) {
        self.stop.store(true, atomic::Ordering::Release)
    }
}