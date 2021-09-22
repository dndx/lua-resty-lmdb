use bitflags::bitflags;
use core::slice::{from_raw_parts, from_raw_parts_mut};
use lmdb::{
    self, Cursor, Database, DatabaseFlags, Environment, InactiveTransaction, RwTransaction,
    Transaction, WriteFlags,
};
use std::collections::HashMap;
use std::error::Error;
use std::ffi::CStr;
use std::io::{ErrorKind, Write};
use std::os::raw::c_char;
use std::path::Path;
use std::ptr;

#[repr(C)]
pub enum ReturnCode {
    OK,
    ERR,
    AGAIN,
}

bitflags! {
    #[repr(C)]
    pub struct LMDBResultFlags: u8 {
        const NOT_FOUND = 0b00000001;
        const AGAIN = 0b00000010;
    }
}

#[repr(C)]
pub struct LMDBOperationArgs {
    key: *const u8,
    key_len: usize,
    value: *mut u8,
    value_len: usize,
    flags: LMDBResultFlags,
}

#[repr(C)]
pub enum LMDBOperationCode {
    Get,
    Set,
    CreateDB,
    DropDB,
    ClearDB,
}

#[repr(C)]
pub struct LMDBOperation {
    op_code: LMDBOperationCode,
    args: LMDBOperationArgs,
}

macro_rules! try_lmdb {
    ( $handle:expr, $expr:expr ) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                $handle.last_err = Some(e);
                return ReturnCode::ERR;
            }
        }
    };
}

pub struct LMDBHandle<'env> {
    env: Environment,
    inactive_txn: Option<InactiveTransaction<'env>>,
    last_err: Option<lmdb::Error>,
    default_db: Database,
    databases: HashMap<String, Database>,
}

#[no_mangle]
pub extern "C" fn ngx_lmdb_handle_get_last_err(handle: &mut LMDBHandle) -> *const u8 {
    match handle.last_err {
        Some(e) => {
            let ptr = e.description().as_ptr();
            handle.last_err = None;
            ptr
        }
        None => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn ngx_lmdb_handle_open<'env>(
    path: *const c_char,
    perm: u32,
    err: *mut *const u8,
) -> *mut LMDBHandle<'env> {
    let path = unsafe { CStr::from_ptr(path).to_str().unwrap() };

    match Environment::new().open_with_permissions(Path::new(path), perm) {
        Ok(env) => {
            let default_db = env.open_db(None).unwrap();
            Box::into_raw(Box::new(LMDBHandle {
                env,
                last_err: None,
                inactive_txn: None,
                default_db,
                databases: HashMap::new(),
            }))
        }
        Err(e) => {
            unsafe {
                *err = e.description().as_ptr();
            }
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn ngx_lmdb_handle_close(handle: *mut LMDBHandle) {
    unsafe { Box::from_raw(handle) };
}

fn execute_lmdb_get<T: Transaction>(
    dbi: Database,
    txn: &T,
    opt: &mut LMDBOperationArgs,
) -> lmdb::Result<()> {
    let key = unsafe { from_raw_parts(opt.key, opt.key_len) };
    let mut value = unsafe { from_raw_parts_mut(opt.value, opt.value_len) };

    match txn.get(dbi, &key) {
        Ok(val) => {
            opt.value_len = val.len();
            if let Err(e) = value.write_all(val) {
                if e.kind() == ErrorKind::WriteZero {
                    opt.flags.insert(LMDBResultFlags::AGAIN);
                } else {
                    // memory writes should never fail for other reasons
                    unreachable!();
                }
            }
        }
        Err(lmdb::Error::NotFound) => {
            opt.flags.insert(LMDBResultFlags::NOT_FOUND);
        }
        Err(e) => return Err(e),
    };

    Ok(())
}

fn execute_lmdb_set(
    dbi: Database,
    txn: &mut RwTransaction,
    opt: &mut LMDBOperationArgs,
) -> lmdb::Result<()> {
    let key = unsafe { from_raw_parts(opt.key, opt.key_len) };
    let value = unsafe { from_raw_parts(opt.value, opt.value_len) };

    if opt.value.is_null() {
        // delete

        match txn.del(dbi, &key, None) {
            Ok(_) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    } else {
        match txn.put(dbi, &key, &value, WriteFlags::empty()) {
            Ok(_) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

fn execute_lmdb_cleardb(dbi: Database, txn: &mut RwTransaction) -> lmdb::Result<()> {
    txn.clear_db(dbi)
}

#[no_mangle]
pub extern "C" fn ngx_lmdb_handle_execute(
    handle: &mut LMDBHandle,
    ops: *mut LMDBOperation,
    n: usize,
    write: bool,
) -> ReturnCode {
    let ops = unsafe { from_raw_parts_mut(ops, n) };
    if write {
        let mut txn = try_lmdb!(handle, handle.env.begin_rw_txn());

        for op in ops {
            op.args.flags = LMDBResultFlags::empty();

            match op.op_code {
                LMDBOperationCode::Get => {
                    try_lmdb!(
                        handle,
                        execute_lmdb_get(handle.default_db, &txn, &mut op.args)
                    );
                }
                LMDBOperationCode::Set => {
                    try_lmdb!(
                        handle,
                        execute_lmdb_set(handle.default_db, &mut txn, &mut op.args)
                    );
                    assert!(op.args.flags.is_empty());
                }
                LMDBOperationCode::ClearDB => {
                    try_lmdb!(handle, execute_lmdb_cleardb(handle.default_db, &mut txn));
                    assert!(op.args.flags.is_empty());
                }
                _ => (),
            };
        }
        try_lmdb!(handle, txn.commit());
    } else {
        let txn = match handle.inactive_txn.take() {
            None => try_lmdb!(handle, handle.env.begin_ro_txn()),
            Some(t) => try_lmdb!(handle, t.renew()),
        };

        for op in ops {
            match op.op_code {
                LMDBOperationCode::Get => {
                    try_lmdb!(
                        handle,
                        execute_lmdb_get(handle.default_db, &txn, &mut op.args)
                    );
                }
                _ => (),
            };
        }
    }

    ReturnCode::OK
}

#[no_mangle]
pub extern "C" fn ngx_lmdb_handle_get_databases<'env>(
    handle: &'env mut LMDBHandle<'env>,
    num_keys: usize,
    values_buf: *mut u8,
    values_buf_len: usize,
    value_lens: *mut i32,
) -> ReturnCode {
    let value_lens = unsafe { from_raw_parts_mut(value_lens, num_keys) };
    let mut values_buf = unsafe { from_raw_parts_mut(values_buf, values_buf_len as usize) };
    let txn = match handle.inactive_txn.take() {
        None => try_lmdb!(handle, handle.env.begin_ro_txn()),
        Some(t) => try_lmdb!(handle, t.renew()),
    };

    let mut cursor = try_lmdb!(handle, txn.open_ro_cursor(handle.default_db));
    for (i, v) in cursor.iter().enumerate() {
        let (key, _val) = try_lmdb!(handle, v);
        value_lens[i] = key.len() as i32;
        if let Err(e) = values_buf.write_all(key) {
            if e.kind() == ErrorKind::WriteZero {
                return ReturnCode::AGAIN;
            }

            return ReturnCode::ERR;
        }
    }

    drop(cursor);

    handle.inactive_txn = Some(txn.reset());
    ReturnCode::OK
}

fn get_database_handle(
    handle: &mut LMDBHandle,
    name: &str,
    create: bool,
) -> lmdb::Result<Database> {
    match handle.databases.get(name) {
        Some(dbi) => Ok(*dbi),
        None => {
            let dbi = if create {
                handle.env.create_db(Some(name), DatabaseFlags::empty())?
            } else {
                handle.env.open_db(Some(name))?
            };
            handle.databases.insert(name.to_string(), dbi);
            Ok(dbi)
        }
    }
}
