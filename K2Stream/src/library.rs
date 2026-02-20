use std::{collections::HashMap, ffi::{CStr, c_char, c_void}, ptr};

use libloading::{Library, Symbol};
use serde::{Deserialize, Serialize};

use crate::{errors::{K2Error, K2ErrorCode}, processors::ProcessorTrait};

pub type ProcessorNew = fn(name: String) -> Result<Box<dyn ProcessorTrait>, K2Error>;

pub type ProcessorNewFFI = extern "C" fn(name: *const c_char) -> *mut c_void;

#[repr(C)]
#[derive(Clone, Serialize, Deserialize)]
pub struct LibraryHeader {
    pub name: String,
    pub description: String,
    pub version: String,
    pub author: String,
    pub email: String,
    pub license: String,
    pub repository: String,
}

#[derive(Clone)]
pub struct LibraryHeaderFFI {
    pub name: *const c_char,
    pub description: *const c_char,
    pub version: *const c_char,
    pub author: *const c_char,
    pub email: *const c_char,
    pub license: *const c_char,
    pub repository: *const c_char,
}

impl From<LibraryHeaderFFI> for LibraryHeader {
    fn from(ffi_header: LibraryHeaderFFI) -> Self {
        LibraryHeader {
            name: unsafe { CStr::from_ptr(ffi_header.name).to_string_lossy().into_owned() },
            description: unsafe { CStr::from_ptr(ffi_header.description).to_string_lossy().into_owned() },
            version: unsafe { CStr::from_ptr(ffi_header.version).to_string_lossy().into_owned() },
            author: unsafe { CStr::from_ptr(ffi_header.author).to_string_lossy().into_owned() },
            email: unsafe { CStr::from_ptr(ffi_header.email).to_string_lossy().into_owned() },
            license: unsafe { CStr::from_ptr(ffi_header.license).to_string_lossy().into_owned() },
            repository: unsafe { CStr::from_ptr(ffi_header.repository).to_string_lossy().into_owned() },
        }
    }
}

#[derive(Clone)]
pub struct LibraryStruct {
    pub header: LibraryHeader,
    processors: HashMap<String, ProcessorNew>,
}
#[repr(C)]
#[derive(Clone)]
pub struct LibraryStructFFI {
    pub header: LibraryHeaderFFI,
    processors: *mut ProcessorEntryFFI,
    processors_len: usize,
}

#[repr(C)]
pub struct ProcessorEntryFFI {
    pub key: *mut c_char,
    pub value: *mut ProcessorNewFFI,
}

impl LibraryStruct {
    pub fn provide(&self) -> Vec<String> {
        self.processors.keys().cloned().collect()
    }
    pub fn new_processor(&self, proc_name: &String, name: &String) -> Option<Box<dyn ProcessorTrait>> {
        let new_callback = self.processors.get(proc_name)?;
        new_callback(name.clone()).ok()
    }

    fn from_ffi(ffi_struct: LibraryStructFFI) -> Self {
        let header = LibraryHeader::from(ffi_struct.header);
        let mut processors = HashMap::new();
        unsafe {
            let entries = std::slice::from_raw_parts(
                ffi_struct.processors,
                ffi_struct.processors_len
            );

            for entry in entries {
                if !entry.key.is_null() {
                    let c_str = CStr::from_ptr(entry.key);
                    let key_string = c_str.to_string_lossy().into_owned();
                    if !entry.value.is_null() {
                        let func_ptr = *entry.value; // Dereferenziamo il puntatore
                        processors.insert(key_string, std::mem::transmute(func_ptr));
                    }
                }
            }
        }
        LibraryStruct {
            header,
            processors,
        }
    }
}

pub struct LibraryHandler {
    libs: HashMap<String, Library>,
    modules: HashMap<String, LibraryStruct>,
}

impl LibraryHandler {
    pub fn new() -> Self {
        LibraryHandler {
            libs: HashMap::new(),
            modules: HashMap::new(),
        }
    }

    pub fn load_library(&mut self, path: &str) -> Result<LibraryStruct, K2Error> {
        // Qui dovresti implementare la logica per caricare la libreria dinamica
        // e ottenere la struttura LibraryStructFFI, poi convertirla in LibraryStruct
        let library: Library = unsafe { libloading::Library::new(path).map_err(|_| K2Error { code: K2ErrorCode::NotFound, message: "Failed to load library".into() })? };

        let module_info: Symbol<*mut LibraryStructFFI>;
        match unsafe { library.get(b"MODULE\0") } {
            Ok(module) => {module_info = module;}
            Err(_) => {
                eprintln!("Unable to find");
                return Err(K2Error { code: K2ErrorCode::NotFound, message: "Failed to find module in library".into() });
            }
        }
        let ptr = *module_info;

        let module: LibraryStruct = unsafe {
            if ptr.is_null() {
                return Err(K2Error { code: K2ErrorCode::NotFound, message: "Module pointer is null".into() });
            }
            let ffi_data: LibraryStructFFI = ptr::read(ptr);
            LibraryStruct::from_ffi(ffi_data)
        };
        let module_name = module.clone().header.name.clone();
        self.modules.insert(module_name.clone(), module.clone());
        self.libs.insert(module_name.clone(), library);
        Ok(module)
    }
}