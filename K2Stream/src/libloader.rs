use std::ffi::c_void;
use libloading::{Library, Symbol};

// Representazione C-compatible of trait object
#[repr(C)]
pub struct TraitObjectRepr {
    pub data: *mut c_void,
    pub vtable: *mut c_void,
}

#[repr(C)]
#[derive(Clone)]
pub struct ModuleHandle<'a> {
    pub module: ModuleStruct,
    pub lib: &'a Library,
    pub get_processor_modules: Symbol<'static, unsafe extern "C" fn(*const u8, usize, *const u8, usize) -> TraitObjectRepr>,
}

impl ModuleHandle<'static> {
    pub fn new(library_path: String) -> Result<Self, ()> {
        let library: &'static Library;
        match unsafe { Library::new(library_path)} {
            Ok(lib) => {
                let box_library = Box::leak(Box::new(lib));
                library = box_library;
            }
            Err(_) => {
                eprintln!("Unable to find");
                return Err(StreamErrCode::FileNotFound);
            }
        }
        let module_info: Symbol<*mut ModuleStructFFI>;
        match unsafe { library.get(b"MODULE\0") } {
            Ok(module) => {module_info = module;}
            Err(_) => {
                eprintln!("Unable to find");
                return Err(StreamErrCode::FileNotFound);
            }
        }
        let module = unsafe{**module_info}.into();
        let funct_ptr: Symbol<unsafe extern "C" fn(*const u8, usize, *const u8, usize) -> TraitObjectRepr>;
        match unsafe { library.get(b"get_processor_modules")} {
            Ok(func) => {funct_ptr = func;}
            Err(_) => {
                eprintln!("Unable to find");
                return Err(StreamErrCode::FileNotFound);
            }
        }
        let handle = Self {
            lib: library,
            module: module,
            get_processor_modules: funct_ptr,
        };
        Ok(handle)
    }
}

pub fn export_processor(proc: Box<dyn ProcessorTrait>) -> TraitObjectRepr {
    let ptr_fat: *mut dyn ProcessorTrait = Box::into_raw(proc);
    unsafe {
        mem::transmute(ptr_fat)
    }
}

pub fn import_processor(repr: TraitObjectRepr) -> Box<dyn ProcessorTrait> {
    unsafe {
        let trait_heap_pointer: *mut dyn ProcessorTrait = mem::transmute(repr);
        Box::from_raw(trait_heap_pointer) 
        
    }
}