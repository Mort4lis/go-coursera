package main

import (
	"fmt"
	"reflect"
)

func i2s(data interface{}, out interface{}) error {
	outVal := reflect.ValueOf(out)
	if outVal.Kind() != reflect.Ptr {
		return fmt.Errorf("out argument type must be a pointer")
	}

	outVal = outVal.Elem()
	if !outVal.CanSet() {
		return fmt.Errorf("out argument must be set")
	}

	switch in := data.(type) {
	case map[string]interface{}:
		return parseStruct(in, outVal)
	case []interface{}:
		return parseSlice(in, outVal)
	default:
		return fmt.Errorf("unknown data argument type %T", data)
	}
}

func parseStruct(in map[string]interface{}, outVal reflect.Value) error {
	if outVal.Kind() != reflect.Struct {
		return fmt.Errorf("out argument type must be a struct")
	}

	outType := outVal.Type()
	for i := 0; i < outVal.NumField(); i++ {
		fieldVal := outVal.Field(i)
		if !fieldVal.CanSet() {
			continue
		}

		fieldType := fieldVal.Type()
		fieldName := outType.Field(i).Name
		inVal, exist := in[fieldName]
		if !exist {
			continue
		}

		switch fieldType.Kind() {
		case reflect.Int:
			var intVal int64

			switch val := inVal.(type) {
			case int:
				intVal = int64(val)
			case float64:
				intVal = int64(val)
			default:
				return fmt.Errorf(
					"unsupport type %T to assign into the struct field %q (type=%q)",
					inVal, fieldName, fieldType,
				)
			}

			fieldVal.SetInt(intVal)
		case reflect.String:
			strVal, ok := inVal.(string)
			if !ok {
				return fmt.Errorf(
					"unsupport type %T to assign into the struct field %q (type=%q)",
					inVal, fieldName, fieldType,
				)
			}

			fieldVal.SetString(strVal)
		case reflect.Bool:
			boolVal, ok := inVal.(bool)
			if !ok {
				return fmt.Errorf(
					"unsupport type %T to assign into the struct field %q (type=%q)",
					inVal, fieldName, fieldType,
				)
			}

			fieldVal.SetBool(boolVal)
		case reflect.Struct:
			m, ok := inVal.(map[string]interface{})
			if !ok {
				return fmt.Errorf("failed to assert type %T to map", inVal)
			}

			if err := parseStruct(m, fieldVal); err != nil {
				return err
			}
		case reflect.Slice:
			sl, ok := inVal.([]interface{})
			if !ok {
				return fmt.Errorf("failed to assert type %T to slice", inVal)
			}

			if err := parseSlice(sl, fieldVal); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupport type %q (fieldName=%q)", fieldType, fieldName)
		}
	}

	return nil
}

func parseSlice(sl []interface{}, outVal reflect.Value) error {
	if outVal.Kind() != reflect.Slice {
		return fmt.Errorf("out argument type must be a slice")
	}

	for _, elem := range sl {
		m, ok := elem.(map[string]interface{})
		if !ok {
			return fmt.Errorf("failed to assert type %T to map", elem)
		}

		v := reflect.New(outVal.Type().Elem()).Elem()
		if err := parseStruct(m, v); err != nil {
			return err
		}

		outVal.Set(reflect.Append(outVal, v))
	}

	return nil
}
