//! Standard slog-rs extensions.
#![warn(missing_docs)]

extern crate slog;
extern crate thread_local;
extern crate take_mut;

use take_mut::take;

use slog::Drain;

use std::sync::{mpsc, Mutex};
use std::fmt;
use std::{io, thread};
use slog::{Record, RecordStatic, Level, SingleKV, KV, BorrowedKV};
use slog::{Serializer, OwnedKVList};


/// `Async` drain
///
/// `Async` will send all the logging records to a wrapped drain running in another thread.
///
/// Note: Dropping `Async` waits for it's worker-thread to finish (thus handle all previous
/// requests). If you can't tolerate the delay, make sure you drop `Async` drain instance eg. in
/// another thread.
pub struct Async {
    ref_sender: Mutex<mpsc::Sender<AsyncMsg>>,
    tl_sender: thread_local::ThreadLocal<mpsc::Sender<AsyncMsg>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Async {
    /// Create `Async` drain
    ///
    /// The wrapped drain must handle all error conditions (`Drain<Error=Never>`). See
    /// `slog::DrainExt::fuse()` and `slog::DrainExt::ignore_err()` for typical error handling
    /// strategies.
    pub fn new<D: slog::Drain<Error=slog::Never> + Send + 'static>(drain: D) -> Self {
        let (tx, rx) = mpsc::channel();
        let join = thread::spawn(move || {
                loop {
                    match rx.recv().unwrap() {
                        AsyncMsg::Record(r) => {
                            let rs = RecordStatic {
                                level: r.level,
                                file: r.file,
                                line: r.line,
                                column: r.column,
                                function: r.function,
                                module: r.module,
                                target: &r.target,
                            };

                            drain.log(
                                &Record::new(&rs,
                                             format_args!("{}", r.msg),
                                             BorrowedKV(&r.kv)
                                            ),
                                            &r.logger_values
                                            ).unwrap();
                        }
                        AsyncMsg::Finish => return,
                    }
                }
        });

        Async{
            ref_sender: Mutex::new(tx),
            tl_sender: thread_local::ThreadLocal::new(),
            join: Mutex::new(Some(join)),
        }
    }

    fn get_sender(&self) -> &mpsc::Sender<AsyncMsg> {
        self.tl_sender.get_or(|| {
            // TODO: Change to `get_or_try` https://github.com/Amanieu/thread_local-rs/issues/2
            Box::new(self.ref_sender.lock().unwrap().clone())
        })
    }

    /// Send `AsyncRecord` to a worker thread.
    fn send(&self, r: AsyncRecord) -> io::Result<()> {
        let sender = self.get_sender();

        sender.send(AsyncMsg::Record(r))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Send failed"))
    }

}

struct ToSendSerializer {
    kv: Box<KV+Send>,
}

impl ToSendSerializer {
    fn new() -> Self {
        ToSendSerializer { kv: Box::new(()) }
    }

    fn finish(self) -> Box<KV+Send> {
        self.kv
    }
}

impl Serializer for ToSendSerializer {
    fn emit_bool(&mut self, key: &str, val: bool) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_unit(&mut self, key: &str) -> slog::Result {
        let val = ();
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_none(&mut self, key: &str) -> slog::Result {
        let val: Option<()> = None;
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_char(&mut self, key: &str, val: char) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_u8(&mut self, key: &str, val: u8) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_i8(&mut self, key: &str, val: i8) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_u16(&mut self, key: &str, val: u16) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_i16(&mut self, key: &str, val: i16) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_u32(&mut self, key: &str, val: u32) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_i32(&mut self, key: &str, val: i32) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_f32(&mut self, key: &str, val: f32) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_u64(&mut self, key: &str, val: u64) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_i64(&mut self, key: &str, val: i64) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_f64(&mut self, key: &str, val: f64) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_usize(&mut self, key: &str, val: usize) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_isize(&mut self, key: &str, val: isize) -> slog::Result {
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_str(&mut self, key: &str, val: &str) -> slog::Result {
        let val = val.to_owned();
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
    fn emit_arguments(&mut self, key: &str, val: &fmt::Arguments) -> slog::Result {
        let val = fmt::format(*val);
        take(&mut self.kv, |kv| Box::new((SingleKV(key.to_owned(), val), kv)));
        Ok(())
    }
}


impl Drain for Async {
    type Error = io::Error;

    fn log(&self, record: &Record, logger_values: &OwnedKVList) -> io::Result<()> {

        let mut ser = ToSendSerializer::new();
        try!(record.kv().serialize(record, &mut ser));

        self.send(AsyncRecord {
            msg: fmt::format(record.msg()),
            level: record.level(),
            file: record.file(),
            line: record.line(),
            column: record.column(),
            function: record.function(),
            module: record.module(),
            target: String::from(record.target()),
            logger_values: logger_values.clone(),
            kv: ser.finish(),
        })
    }
}

struct AsyncRecord {
    msg: String,
    level: Level,
    file: &'static str,
    line: u32,
    column: u32,
    function: &'static str,
    module: &'static str,
    target: String,
    logger_values: OwnedKVList,
    kv: Box<KV+Send>,
}

enum AsyncMsg {
    Record(AsyncRecord),
    Finish,
}

impl Drop for Async {
    fn drop(&mut self) {
        let sender = self.get_sender();

        let _ = sender.send(AsyncMsg::Finish);
        let _ = self.join.lock().unwrap().take().unwrap().join();
    }
}
