// MIT License
//
// Copyright (c) 2018-2020 Hans-Martin Will
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

// JSON tokenizer (per http://www.json.org/)
//
// object
//  {}
//  { members }
//
// members
//  pair
//  pair , members
//
// pair
//  string : value
//
// array
//  []
//  [ elements ]
//
// elements
//  value
//  value , elements
//
// value
//  string
//  number
//  object
//  array
//  true
//  false
//  null
//
// string
//  ""
//  " chars "
//
// chars
//  char
//  char chars
//
// char
//  any-Unicode-character-
//     except-"-or-\-or-
//     control-character
//  \"
//  \\
//  \/
//  \b
//  \f
//  \n
//  \r
//  \t
//  \u four-hex-digits
//
// number
//  int
//  int frac
//  int exp
//  int frac exp
//
// int
//  digit
//  digit1-9 digits
//  - digit
//  - digit1-9 digits
//
// frac
//  . digits
//
// exp
//  e digits
//
// digits
//  digit
//  digit digits
//
// e
//  e
//  e+
//  e-
//  E
//  E+
//  E-

use std::char;
use std::ops;
use std::str;

pub type TokenizerError = String;
pub type TokenizerResult = Result<(), TokenizerError>;

#[derive(PartialEq, Eq)]
pub enum Token {
    Eof,
    Error,
    Null,
    True,
    False,
    String,
    Number,
    BeginObject,
    EndObject,
    BeginArray,
    EndArray,
    Comma,
    Colon,
}

/// A tokenizer for JSON input; see [JSON Specification](http://www.json.org/) for more details.
pub struct Tokenizer<'a> {
    pub token: Token,
    input: &'a str,
    iterator: str::CharIndices<'a>,
    look_ahead: Option<(usize, char)>,
    value_range: ops::Range<usize>,
    string_buffer: String,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Result<Tokenizer<'a>, TokenizerError> {
        let mut iterator = input.char_indices();
        let look_ahead = iterator.next();

        let mut result = Tokenizer {
            input,
            iterator,
            look_ahead,
            token: Token::Eof,
            value_range: 0..0,
            string_buffer: String::new(),
        };

        result.next()?;
        Ok(result)
    }

    fn advance(&mut self) {
        self.look_ahead = self.iterator.next()
    }

    fn current_pos(&self) -> usize {
        match self.look_ahead {
            Some((index, _)) => index,
            None => self.input.len(),
        }
    }
    fn mark_start(&mut self) {
        let pos = self.current_pos();
        self.value_range = pos..pos
    }

    fn mark_end(&mut self) {
        let pos = self.current_pos();
        self.value_range = self.value_range.start..pos;
    }

    fn skip_whitespace(&mut self) {
        while let Some((_, ch)) = self.look_ahead {
            if !ch.is_whitespace() {
                break;
            }

            self.advance()
        }
    }

    fn single_char_token(&mut self, token: Token) -> TokenizerResult {
        self.token = token;
        self.advance();
        Ok(())
    }

    fn match_keyword(&mut self, string: &str, token: Token) -> TokenizerResult {
        for target in string.chars() {
            if let Some((_, ch)) = self.look_ahead {
                if ch == target {
                    self.advance()
                } else {
                    self.token = Token::Error;
                    return Err(String::from("Invalid token"));
                }
            } else {
                self.token = Token::Error;
                return Err(String::from("Invalid token"));
            }
        }

        // Ensure that the look-ahead character does not look like an identifier
        match self.look_ahead {
            Some((_, ch)) if ch.is_alphanumeric() => {
                self.token = Token::Error;
                Err(String::from("Invalid token"))
            }
            _ => {
                self.token = token;
                Ok(())
            }
        }
    }

    fn push_string_char(&mut self, ch: char) {
        self.string_buffer.push(ch);
        self.advance();
    }

    fn string_control_char(&mut self) -> Result<(), TokenizerError> {
        // eat '\\'
        self.advance();

        match self.look_ahead {
            Some((_, '/')) => Ok(self.push_string_char('/')),
            Some((_, '\\')) => Ok(self.push_string_char('\\')),
            Some((_, '"')) => Ok(self.push_string_char('"')),
            Some((_, 'b')) => Ok(self.push_string_char('\u{8}')),
            Some((_, 'f')) => Ok(self.push_string_char('\u{c}')),
            Some((_, 'n')) => Ok(self.push_string_char('\n')),
            Some((_, 'r')) => Ok(self.push_string_char('\r')),
            Some((_, 't')) => Ok(self.push_string_char('\t')),
            Some((_, 'u')) => {
                self.advance();

                let mut char_code = 0u32;

                // need for hexadecimal digits
                for _ in 0..4 {
                    match self.look_ahead {
                        Some((_, ch)) => {
                            if let Some(digit) = ch.to_digit(16) {
                                char_code = char_code * 16 + digit;
                                self.advance();
                            }
                        }
                        _ => {
                            return Err(String::from(
                                "Invalid Unicode escape sequence in string value",
                            ))
                        }
                    }
                }

                // 16 bit should be the safe range for Unicode characters
                self.string_buffer.push(char::from_u32(char_code).unwrap());
                Ok(())
            }
            _ => Err(String::from("Invalid escape sequence in string value")),
        }
    }

    fn match_string(&mut self) -> TokenizerResult {
        // markers are used for error token
        self.mark_start();
        self.advance();
        self.string_buffer.clear();

        while let Some((_, ch)) = self.look_ahead {
            match ch {
                '"' => {
                    self.advance();
                    self.token = Token::String;
                    return Ok(());
                }
                '\\' => self.string_control_char()?,
                ch if ch <= 0x1fu8 as char => break,
                _ => {
                    self.string_buffer.push(ch);
                    self.advance();
                }
            }
        }

        // error case
        self.mark_end();
        self.token = Token::Error;
        Err(String::from(
            "Missing closing quotation mark in string value",
        ))
    }

    fn digits(&mut self) {
        while let Some((_, ch)) = self.look_ahead {
            if ch.is_digit(10) {
                self.advance()
            } else {
                break;
            }
        }
    }

    fn match_number(&mut self) -> TokenizerResult {
        self.mark_start();

        // check for a negative sign
        if let Some((_, '-')) = self.look_ahead {
            self.advance()
        }

        // digit sequence; strictly speaking, no leading zero is allowed
        match self.look_ahead {
            Some((_, '0')) => self.advance(),
            Some((_, ch)) if ch.is_digit(10) => {
                self.advance();
                self.digits()
            }
            _ => return Err(String::from("Invalid number token")),
        }

        // check for fractional part
        if let Some((_, '.')) = self.look_ahead {
            self.advance();
            self.digits();
        }

        // check for exponent part
        match self.look_ahead {
            Some((_, 'e')) | Some((_, 'E')) => {
                self.advance();

                match self.look_ahead {
                    Some((_, '+')) | Some((_, '-')) => self.advance(),
                    _ => (),
                }

                match self.look_ahead {
                    Some((_, ch)) if ch.is_digit(10) => self.digits(),
                    _ => return Err(String::from("Invalid exponent format for number")),
                }
            }
            _ => (),
        }

        self.mark_end();
        self.token = Token::Number;
        Ok(())
    }

    pub fn value(&self) -> &str {
        if self.token == Token::String {
            self.string_buffer.as_str()
        } else {
            let range = self.value_range.clone();
            &self.input.get(range).unwrap()
        }
    }

    /// Advance the tokenizer
    ///
    /// Returns an error in case there was a problem.
    pub fn next(&mut self) -> TokenizerResult {
        self.skip_whitespace();

        match self.look_ahead {
            None => {
                self.token = Token::Eof;
                Ok(())
            }
            Some((_, '[')) => self.single_char_token(Token::BeginArray),
            Some((_, ']')) => self.single_char_token(Token::EndArray),
            Some((_, '{')) => self.single_char_token(Token::BeginObject),
            Some((_, '}')) => self.single_char_token(Token::EndObject),
            Some((_, ',')) => self.single_char_token(Token::Comma),
            Some((_, ':')) => self.single_char_token(Token::Colon),

            // Null
            Some((_, 'n')) => self.match_keyword("null", Token::Null),

            // True
            Some((_, 't')) => self.match_keyword("true", Token::True),

            // False
            Some((_, 'f')) => self.match_keyword("false", Token::False),

            // String
            Some((_, '"')) => self.match_string(),

            // Number
            Some((_, '-')) => self.match_number(),
            Some((_, ch)) if ch.is_digit(10) => self.match_number(),

            _ => {
                self.token = Token::Error;
                Err(String::from("Invalid JSON token"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Token, Tokenizer};

    #[test]
    fn smoketest() {
        let input = " []{},:1234.34 -123 \"Hallo\" true false null";
        let mut tokenizer = Tokenizer::new(input).unwrap();
        assert!(tokenizer.token == Token::BeginArray);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::EndArray);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::BeginObject);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::EndObject);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::Comma);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::Colon);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::Number);
        assert!(tokenizer.value() == "1234.34");
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::Number);
        assert!(tokenizer.value() == "-123");
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::String);
        assert!(tokenizer.value() == "Hallo");
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::True);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::False);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::Null);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::Eof);
        assert!(tokenizer.next().is_ok());

        assert!(tokenizer.token == Token::Eof);
    }

    // Helper function to test string tokens
    fn is_value_token(input: &str, index: usize, value: &str, token: Token) -> bool {
        if let Ok(mut tokenizer) = Tokenizer::new(input) {
            for _ in 0..index {
                if tokenizer.token == Token::Error {
                    return false;
                }

                if tokenizer.next().is_err() {
                    return false;
                }
            }

            if tokenizer.token != token {
                return false;
            }

            return tokenizer.value() == value;
        } else {
            false
        }
    }

    fn is_string(input: &str, index: usize, value: &str) -> bool {
        is_value_token(input, index, value, Token::String)
    }

    fn is_number(input: &str, index: usize, value: &str) -> bool {
        is_value_token(input, index, value, Token::Number)
    }

    #[test]
    fn test_strings() {
        assert!(is_string(" \"abc\" ", 0, "abc"));
        assert!(is_string(": null \"abc\" ", 2, "abc"));
        assert!(is_string(
            r#"["\u0060\u012a\u12AB"]"#,
            1,
            "\u{60}\u{12a}\u{12ab}"
        ));
        assert!(is_string(
            r#"["\"\\\/\b\f\n\r\t"]"#,
            1,
            "\"\\/\u{8}\u{c}\n\r\t"
        ));
        assert!(is_string(r#"["\\u0000"]"#, 1, "\\u0000"));
        assert!(is_string(r#"["\""]"#, 1, "\""));
        assert!(is_string(r#"["a/*b*/c/*d//e"]"#, 1, "a/*b*/c/*d//e"));
        assert!(is_string(r#"["\\a"]"#, 1, "\\a"));
        assert!(is_string(r#"["\\n"]"#, 1, "\\n"));
        assert!(is_string(r#"["\u0012"]"#, 1, "\u{12}"));
        assert!(is_string(r#"["\uFFFF"]"#, 1, "\u{ffff}"));
        assert!(is_string(r#"[ "asd"]"#, 1, "asd"));
        assert!(is_string(r#"["new\u00A0line"]"#, 1, "new\u{a0}line"));
        assert!(is_string(r#"["􏿿"]"#, 1, "􏿿"));
        assert!(is_string(r#"["￿"]"#, 1, "￿"));
        assert!(is_string(r#"["\u0000"]"#, 1, "\u{0}"));
        assert!(is_string(r#"["\u002c"]"#, 1, "\u{2c}"));
        assert!(is_string(r#"["π"]"#, 1, "π"));
        assert!(is_string(r#"["𛿿"]"#, 1, "𛿿"));
        assert!(is_string(r#"["asd "]"#, 1, "asd "));
        assert!(is_string(r#"["\u0821"]"#, 1, "\u{821}"));
        assert!(is_string(r#"["\u0123"]"#, 1, "\u{123}"));
        assert!(is_string(r#"[""]"#, 1, ""));
        assert!(is_string(r#"[""]"#, 1, ""));
        assert!(is_string(
            r#"["\u0061\u30af\u30EA\u30b9"]"#,
            1,
            "\u{61}\u{30af}\u{30ea}\u{30b9}"
        ));
        assert!(is_string(r#"["new\u000Aline"]"#, 1, "new\u{a}line"));
        assert!(is_string(r#"[""]"#, 1, ""));
        assert!(is_string(r#"["\uA66D"]"#, 1, "\u{a66d}"));
        assert!(is_string(r#"["\u005C"]"#, 1, "\u{5c}"));
        assert!(is_string(r#"["⍂㈴⍂"]"#, 1, "⍂㈴⍂"));
        assert!(is_string(r#"["\u200B"]"#, 1, "\u{200b}"));
        assert!(is_string(r#"["\u2064"]"#, 1, "\u{2064}"));
        assert!(is_string(r#"["\uFDD0"]"#, 1, "\u{fdd0}"));
        assert!(is_string(r#"["\uFFFE"]"#, 1, "\u{fffe}"));
        assert!(is_string(r#"["\u0022"]"#, 1, "\u{22}"));
        assert!(is_string(r#"["€𝄞"]"#, 1, "€𝄞"));
        assert!(is_string(r#"["aa"]"#, 1, "aa"));

        // TODO: Verify handling of surrogate pairs
        //assert!(is_string(r#"["\uD801\udc37"]"#, 1, "\u{db01}\u{dc37}"));
        //assert!(is_string(r#"["\ud83d\ude39\ud83d\udc8d"]"#, 1,
        // "\u{d83d}\u{de39}\u{d83d}\u{dc8d}"));
        // assert!(is_string(r#"["\uDBFF\uDFFF"]"#, 1, "\u{dbff}\u{dfff}"));
        //assert!(is_string(r#"["\uD834\uDd1e"]"#, 1, "\u{d834}\u{dd1e}"));
        //assert!(is_string(r#"["\uDBFF\uDFFE"]"#, 1, "\u{dbff}\u{dffe}"));
        //assert!(is_string(r#"["\uD83F\uDFFE"]"#, 1, "\u{d83f}\u{dffe}"));
    }

    #[test]
    fn test_numbers() {
        assert!(is_number(" 123.45 ", 0, "123.45"));
        assert!(is_number(": null -764.87 ", 2, "-764.87"));

        // TODO: Add more test cases from JSON test suite
        assert!(is_number("[123e65]", 1, "123e65"));
        assert!(is_number("[0e+1]", 1, "0e+1"));
        assert!(is_number("[0e1]", 1, "0e1"));
        assert!(is_number("[ 4]", 1, "4"));
        assert!(is_number(
            "[-0.000000000000000000000000000000000000000000000000000000000000000000000000000001]",
            1,
            "-0.000000000000000000000000000000000000000000000000000000000000000000000000000001"
        ));
        assert!(is_number("[20e1]", 1, "20e1"));
        assert!(is_number("[-0]", 1, "-0"));
        assert!(is_number("[-123]", 1, "-123"));
        assert!(is_number("[-1]", 1, "-1"));
        assert!(is_number("[1E22]", 1, "1E22"));
        assert!(is_number("[1E-2]", 1, "1E-2"));
        assert!(is_number("[1E+2]", 1, "1E+2"));
        assert!(is_number("[123e45]", 1, "123e45"));
        assert!(is_number("[123.456e78]", 1, "123.456e78"));
        assert!(is_number("[1e-2]", 1, "1e-2"));
        assert!(is_number("[1e+2]", 1, "1e+2"));
        assert!(is_number("[123]", 1, "123"));
        assert!(is_number("[123.456789]", 1, "123.456789"));
    }
}
