// Package copyx provides functionality to perform deep copies of complex data structures.
package copyx

import (
	"reflect"
)

// DeepCopy performs a deep copy from the source (src) to the destination (dst).
// It uses reflection to recursively copy all fields of the source object,
// ensuring that nested structures are also duplicated rather than simply referenced.
// dst and src must be pointers to the same type.
func DeepCopy(dst, src interface{}) {
	dstValue := reflect.ValueOf(dst).Elem()
	srcValue := reflect.ValueOf(src).Elem()

	// Call the recursive deep copy function to handle the actual copying of values.
	deepCopyValue(dstValue, srcValue)
}

// deepCopyValue is a recursive helper function that performs the actual deep copy logic
// for various kinds of values, including pointers, structs, slices, arrays, and maps.
func deepCopyValue(dst, src reflect.Value) {
	switch src.Kind() {
	case reflect.Ptr:
		if !src.IsNil() {
			dst.Set(reflect.New(src.Elem().Type()))
			deepCopyValue(dst.Elem(), src.Elem())
		}
	case reflect.Struct:
		for i := 0; i < src.NumField(); i++ {
			deepCopyValue(dst.Field(i), src.Field(i))
		}
	case reflect.Slice:
		if !src.IsNil() {
			dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Cap()))
			for i := 0; i < src.Len(); i++ {
				deepCopyValue(dst.Index(i), src.Index(i))
			}
		}
	case reflect.Array:
		for i := 0; i < src.Len(); i++ {
			deepCopyValue(dst.Index(i), src.Index(i))
		}
	case reflect.Map:
		if !src.IsNil() {
			dst.Set(reflect.MakeMap(src.Type()))
			for _, key := range src.MapKeys() {
				dstValue := reflect.New(src.MapIndex(key).Type()).Elem()
				deepCopyValue(dstValue, src.MapIndex(key))
				dst.SetMapIndex(key, dstValue)
			}
		}
	default:
		dst.Set(src)
	}
}
