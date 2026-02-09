use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(K2Memory)]
pub fn processor_macro_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;
    let generics = &ast.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let expanded = quote! {
        impl #impl_generics MemoryTrait for #name #ty_generics #where_clause {
            fn as_any(&self) -> &dyn std::any::Any {self}
            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {self}
        }
    };
    expanded.into()
}
