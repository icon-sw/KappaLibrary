use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(K2ProcessorBlock)]
pub fn processor_macro_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;
    let generics = &ast.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let expanded = quote! {
        impl #impl_generics ProcessorBlockTrait for #name #ty_generics #where_clause {
            fn name(&self) -> &DataHeader { &self.name}
            fn proc_name(&self) -> &String { &self.header().proc_name }
            fn header(&self) -> &ProcessorHeader { &self.header }
            fn lock() -> Result<MutexGuard<'static, Self>, ()> where Self: Sized {Err(())}
            fn get_proc_state(&self) -> Result<StreamState, ()> {
                let state = self.state.lock().map_err(|_| ())?;
                Ok((*state).clone())
            }
            fn get_stream_block(&self) -> &StreamBlock {
                &self.stream_block
            }
            fn get_stream_block_mut(&mut self) -> &mut StreamBlock {
                &mut self.stream_block
            }
        }
    };
    expanded.into()
}
