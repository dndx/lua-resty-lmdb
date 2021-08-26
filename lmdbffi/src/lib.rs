use core::slice::{from_raw_parts, from_raw_parts_mut};
use lmdb::{
    self, Database, DatabaseFlags, Environment, InactiveTransaction, Transaction, WriteFlags,
};
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
            let default_db = env.create_db(None, DatabaseFlags::empty()).unwrap();
            Box::into_raw(Box::new(LMDBHandle {
                env,
                last_err: None,
                inactive_txn: None,
                default_db,
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

#[no_mangle]
pub extern "C" fn ngx_lmdb_handle_get_multi<'env>(
    handle: &'env mut LMDBHandle<'env>,
    num_keys: usize,
    key_ptrs: *const *const u8,
    key_lens: *mut u32,
    values_buf: *mut u8,
    values_buf_len: usize,
    value_lens: *mut i32,
) -> ReturnCode {
    let key_lens = unsafe { from_raw_parts(key_lens, *key_lens as usize) };
    let value_lens = unsafe { from_raw_parts_mut(value_lens, num_keys) };
    let key_ptrs = unsafe { from_raw_parts(key_ptrs, num_keys as usize) };
    let mut values_buf = unsafe { from_raw_parts_mut(values_buf, values_buf_len as usize) };
    let txn = match handle.inactive_txn.take() {
        None => try_lmdb!(handle, handle.env.begin_ro_txn()),
        Some(t) => try_lmdb!(handle, t.renew()),
    };

    for i in 0..num_keys {
        let k = unsafe { from_raw_parts(key_ptrs[i], key_lens[i] as usize) };
        match txn.get(handle.default_db, &k) {
            Ok(val) => {
                value_lens[i] = val.len() as i32;
                if let Err(e) = values_buf.write_all(val) {
                    if e.kind() == ErrorKind::WriteZero {
                        return ReturnCode::AGAIN;
                    }

                    return ReturnCode::ERR;
                }
            }
            Err(lmdb::Error::NotFound) => {
                value_lens[i] = -1;
            }
            Err(e) => {
                handle.last_err = Some(e);
                return ReturnCode::ERR;
            }
        }
    }

    handle.inactive_txn = Some(txn.reset());

    ReturnCode::OK
}

#[no_mangle]
pub extern "C" fn ngx_lmdb_handle_set_multi(
    handle: &mut LMDBHandle,
    num_keys: usize,
    key_ptrs: *const *const u8,
    key_lens: *mut u32,
    value_ptrs: *const *const u8,
    value_lens: *mut u32,
) -> ReturnCode {
    let key_ptrs = unsafe { from_raw_parts(key_ptrs, num_keys) };
    let key_lens = unsafe { from_raw_parts(key_lens, num_keys) };
    let value_ptrs = unsafe { from_raw_parts(value_ptrs, num_keys) };
    let value_lens = unsafe { from_raw_parts(value_lens, num_keys) };
    let mut txn = try_lmdb!(handle, handle.env.begin_rw_txn());

    for i in 0..num_keys {
        let key = unsafe { from_raw_parts(key_ptrs[i], key_lens[i] as usize) };
        let value = unsafe { from_raw_parts(value_ptrs[i], value_lens[i] as usize) };

        if value_ptrs[i].is_null() {
            // delete

            match txn.del(handle.default_db, &key, None) {
                Ok(_) | Err(lmdb::Error::NotFound) => {}
                Err(e) => {
                    handle.last_err = Some(e);
                    return ReturnCode::ERR;
                }
            }
        } else {
            match txn.put(handle.default_db, &key, &value, WriteFlags::empty()) {
                Ok(_) | Err(lmdb::Error::NotFound) => {}
                Err(e) => {
                    handle.last_err = Some(e);
                    return ReturnCode::ERR;
                }
            }
        }
    }

    try_lmdb!(handle, txn.commit());
    ReturnCode::OK
}
