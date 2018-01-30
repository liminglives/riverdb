#include <Python.h> 
#include "riverdb.h"

typedef struct {
    PyObject_HEAD
    void * reader;
    //bool header_readed;
    int col_num;
    int row_num;
} RiverdbPyReader;

typedef struct {
    PyObject_HEAD
    void * writer;
} RiverdbPyWriter;

static PyObject* RiverdbPyReader_iter(PyObject* self) {
    Py_INCREF(self);
    return self;
}

static void close_reader(RiverdbPyReader* binary_reader) {
    if (binary_reader && binary_reader->reader) {
        delete (RiverDB::RiverDBReader*)(binary_reader->reader);
        binary_reader->reader = NULL;
    }
}

static void RiverdbPyReader_dealloc(PyObject* self) {
    RiverdbPyReader *binary_reader = (RiverdbPyReader *)self;
    close_reader(binary_reader);
    Py_TYPE(self)->tp_free(self);
}

PyObject* RiverdbPyReader_close(PyObject* self) {
    RiverdbPyReader* binary_reader = (RiverdbPyReader *)self;
    close_reader(binary_reader);
    return Py_BuildValue("i", 0);
}


static PyObject* RiverdbPyReader_iternext(PyObject* self) {
    RiverdbPyReader *binary_reader = (RiverdbPyReader *)self;
    RiverDB::RiverDBReader * reader = (RiverDB::RiverDBReader*)(binary_reader->reader);
    //if (!binary_reader->header_readed) {
    //    reader->read_header();
    //    binary_reader->header_readed = true;
    //    binary_reader->col_num = reader->get_col_size();
    //}
    
    std::vector<std::string> row_vals;
    row_vals.reserve(binary_reader->col_num);
    if (reader->read_row(row_vals) == RiverDB::RET_OK) {
        PyObject* row = PyList_New(binary_reader->col_num);
        for (unsigned int i = 0; i < row_vals.size(); ++i) {
            if (row_vals[i].size() == 0) {
                PyList_SetItem(row, i, Py_BuildValue(""));
            } else if (reader->get_col_datatype(i) == RiverDB::DT_INT16) {
                int16_t val = 0;
                reader->get_value<int16_t>(row_vals[i], &val);
                PyList_SetItem(row, i, Py_BuildValue("h", val));

            } else if (reader->get_col_datatype(i) == RiverDB::DT_INT32) {
                int32_t val = 0;
                reader->get_value<int32_t>(row_vals[i], &val);
                PyList_SetItem(row, i, Py_BuildValue("i", val));
            } else if (reader->get_col_datatype(i) == RiverDB::DT_INT64) {
                int64_t val = 0;
                reader->get_value<int64_t>(row_vals[i], &val);
                PyList_SetItem(row, i, Py_BuildValue("l", val));
            } else if (reader->get_col_datatype(i) == RiverDB::DT_UINT16) {
                uint16_t val = 0;
                reader->get_value<uint16_t>(row_vals[i], &val);
                PyList_SetItem(row, i, Py_BuildValue("H", val));
            } else if (reader->get_col_datatype(i) == RiverDB::DT_UINT32) {
                uint32_t val = 0;
                reader->get_value<uint32_t>(row_vals[i], &val);
                PyList_SetItem(row, i, Py_BuildValue("I", val));
            } else if (reader->get_col_datatype(i) == RiverDB::DT_UINT64) {
                uint64_t val = 0;
                reader->get_value<uint64_t>(row_vals[i], &val);
                PyList_SetItem(row, i, Py_BuildValue("k", val));
            } else if (reader->get_col_datatype(i) == RiverDB::DT_FLOAT) {
                float val = 0;
                reader->get_value<float>(row_vals[i], &val);
                PyList_SetItem(row, i, Py_BuildValue("d", val));
            } else if (reader->get_col_datatype(i) == RiverDB::DT_DOUBLE) {
                double val = 0;
                reader->get_value<double>(row_vals[i], &val);
                PyList_SetItem(row, i, Py_BuildValue("d", val));
            } else if (reader->get_col_datatype(i) == RiverDB::DT_STRING) {
                std::string val;
                reader->get_value<std::string>(row_vals[i], &val);
                PyList_SetItem(row, i, Py_BuildValue("s", val.c_str()));
            } else {
                PyList_SetItem(row, i, Py_BuildValue("s", "unknown"));
            }
        }
        return row;

    } else {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
}

PyObject* RiverdbPyReader_get_col_num(PyObject* self) {
    RiverdbPyReader* csv_reader = (RiverdbPyReader *)self;
    return Py_BuildValue("i", csv_reader->col_num);
}

PyObject* RiverdbPyReader_get_row_num(PyObject* self) {
    RiverdbPyReader* csv_reader = (RiverdbPyReader *)self;
    return Py_BuildValue("i", csv_reader->row_num);
}

PyObject* RiverdbPyReader_get_index(PyObject* self) {
    RiverdbPyReader* csv_reader = (RiverdbPyReader *)self;
    RiverDB::RiverDBReader* reader = (RiverDB::RiverDBReader*)(csv_reader->reader);

    const std::vector<RiverDB::ColMeta>& col_metas = reader->get_col_metas();
    PyObject* index = PyList_New(col_metas.size()); 
    for (unsigned int i = 0; i < col_metas.size(); ++i) {
        PyList_SetItem(index, i, Py_BuildValue("s", (col_metas[i].name).c_str()));
    }

    return index;
}

PyObject* RiverdbPyReader_get_dtype(PyObject* self) {
    RiverdbPyReader* csv_reader = (RiverdbPyReader *)self;
    RiverDB::RiverDBReader* reader = (RiverDB::RiverDBReader*)(csv_reader->reader);

    const std::vector<RiverDB::ColMeta>& col_metas = reader->get_col_metas();
    PyObject* dtype = PyList_New(col_metas.size()); 
    for (unsigned int i = 0; i < col_metas.size(); ++i) {
        PyList_SetItem(dtype, i, Py_BuildValue("i", (col_metas[i].type)));
    }

    return dtype;
}

static PyMethodDef ReaderSelfMethods[] = {
    {"col_num", (PyCFunction)RiverdbPyReader_get_col_num, METH_NOARGS, ""},
    {"row_num", (PyCFunction)RiverdbPyReader_get_row_num, METH_NOARGS, ""},
    {"index", (PyCFunction)RiverdbPyReader_get_index, METH_NOARGS, ""},
    {"dtype", (PyCFunction)RiverdbPyReader_get_dtype, METH_NOARGS, ""},
    {"close", (PyCFunction)RiverdbPyReader_close, METH_NOARGS, ""},
    {NULL, NULL, 0, NULL}

};

static PyTypeObject RiverdbReader_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "riverdb._reader",      /*tp_name*/
    sizeof(RiverdbPyReader),   /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    RiverdbPyReader_dealloc,   /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER,
      /* tp_flags: Py_TPFLAGS_HAVE_ITER tells python to
         use tp_iter and tp_iternext fields. */
    "Internal myiter iterator object.",           /* tp_doc */
    0,  /* tp_traverse */
    0,  /* tp_clear */
    0,  /* tp_richcompare */
    0,  /* tp_weaklistoffset */
    RiverdbPyReader_iter,  /* tp_iter: __iter__() method */
    RiverdbPyReader_iternext, /* tp_iternext: next() method */
    ReaderSelfMethods /* methods */
};


static PyObject * riverdb_reader(PyObject* self, PyObject* args) {
    const char* s;
    if (!PyArg_ParseTuple(args, "s", &s)) {
        return NULL;
    }

    RiverdbPyReader* binary_reader;
    binary_reader = PyObject_New(RiverdbPyReader, &RiverdbReader_Type);
    if (binary_reader == NULL) {
        return NULL;
    }

    if (!PyObject_Init((PyObject*)binary_reader, &RiverdbReader_Type)) {
        Py_DECREF(binary_reader);
        return NULL;
    }

    RiverDB::RiverDBReader* reader = NULL;
    try {
        reader = new RiverDB::RiverDBReader(s);
        if (reader == NULL) {
            Py_DECREF(binary_reader);
            return NULL;
        }
        if (reader->init() != RiverDB::RET_OK) {
            delete reader;
            Py_DECREF(binary_reader);
            return NULL;
        }
    } catch (RiverDB::RTTException& e) {
        std::cout << e.info() << std::endl;
        delete reader;
        Py_DECREF(binary_reader);
        return NULL;
    }

    binary_reader->reader = (void *)reader;
    binary_reader->col_num = reader->get_col_size();
    binary_reader->row_num = reader->get_row_size();;
    
    return (PyObject *)binary_reader;
}

/////   writer

static void close_writer(RiverdbPyWriter* binary_writer) {
    if (binary_writer != NULL && binary_writer->writer != NULL) {
        delete (RiverDB::RiverDBWriter*)(binary_writer->writer);
        binary_writer->writer = NULL;
    }
}

static void RiverdbPyWriter_dealloc(PyObject* self) {
    RiverdbPyWriter *binary_writer = (RiverdbPyWriter *)self;
    close_writer(binary_writer);
    Py_TYPE(self)->tp_free(self);
}

static int push_row(RiverDB::RiverDBWriter* writer, int index, PyObject* item, 
        std::vector<std::string>& row) {
    int type = writer->get_data_type(index);
    if (type == -1) {
        return -1;
    }
    if ((PyInt_Check(item) || PyLong_Check(item)) && (type >= RiverDB::DT_INT16 && type <= RiverDB::DT_UINT64)) {
        long val_l = 0;
        unsigned long val_ul = 0;
        if (PyInt_Check(item)) {
            val_l = PyInt_AsLong(item);
            val_ul = PyInt_AsUnsignedLongMask(item);
        } else {
            val_l = PyLong_AsLong(item);
            val_ul = PyLong_AsUnsignedLong(item);
        }

        if (type == RiverDB::DT_INT16) {
            writer->push_row<int16_t>((int16_t)val_l, row);
        } else if (type == RiverDB::DT_INT32) {
            writer->push_row<int32_t>((int32_t)val_l, row);
        } else if (type == RiverDB::DT_INT64) {
            writer->push_row<int64_t>((int64_t)val_l, row);
        } else if (type == RiverDB::DT_UINT16) {
            writer->push_row<uint16_t>((uint16_t)val_ul, row);
        } else if (type == RiverDB::DT_UINT32) {
            writer->push_row<uint32_t>((uint32_t)val_ul, row);
        } else if (type == RiverDB::DT_UINT64) {
            writer->push_row<uint64_t>((uint64_t)val_ul, row);
        } else {
            std::cout << "index=" << index << " int unknown type:" << type << std::endl;
            return -1;
        }
    } else if (PyFloat_Check(item) && (type == RiverDB::DT_FLOAT || type == RiverDB::DT_DOUBLE)) {
        double val = PyFloat_AsDouble(item); 
        if (type == RiverDB::DT_FLOAT) {
            writer->push_row<float>((float)val, row);
        } else if (type == RiverDB::DT_DOUBLE) {
            writer->push_row<double>(val, row);
        } else {
            std::cout << "index=" << index << " float unknown type:" << type << std::endl;
            return -1;
        }
    } else if (PyString_Check(item) && type == RiverDB::DT_STRING) {
        const char* str = PyString_AsString(item);
        writer->push_row<std::string>(str, row);
    } else if (item == Py_None) {
        RiverDB::EmptyValue empty_value;
        writer->push_row<RiverDB::EmptyValue>(empty_value, row);
    } else {
        std::cout << "index=" << index << " unsupported python type " << type << std::endl;
        return -1;
    }

    return 0;
}

static PyObject* RiverdbPyWriter_write_row(PyObject* self, PyObject* args) {
    PyObject* list;
    if (!PyArg_ParseTuple(args, "O", &list)) {
        std::cout << "parse failed" << std::endl;
        return NULL;
    }

    if (!PyList_Check(list)) {
        std::cout << "param is not list type" << std::endl;
        return Py_BuildValue("i", -1);;
    }

    RiverDB::RiverDBWriter * writer = (RiverDB::RiverDBWriter*)(((RiverdbPyWriter *)self)->writer);

    try {
        int size = PyList_Size(list);
        std::vector<std::string> row;
        row.reserve(writer->get_col_size());
        for (int i = 0; i < size; ++i) {
            PyObject* item = PyList_GetItem(list, i);
            if (0 != push_row(writer, i, item, row)) {
                return Py_BuildValue("i", -1);
            }
        }
        if (RiverDB::RET_OK != writer->write_row(row)) {
            std::cout << "writer row error" << std::endl;
            return Py_BuildValue("i", -1);
        }
    } catch (RiverDB::RTTException& e) {
        std::cout << e.info() << std::endl;
        return Py_BuildValue("i", -1);
    }

    return Py_BuildValue("i", 0);
}

static PyObject* RiverdbPyWriter_flush(PyObject* self) {
    RiverDB::RiverDBWriter * writer = (RiverDB::RiverDBWriter*)(((RiverdbPyWriter *)self)->writer);
    writer->flush();
    return Py_BuildValue("i", 0);
}

static PyObject* RiverdbPyWriter_close(PyObject* self) {
    close_writer((RiverdbPyWriter *)self);
    return Py_BuildValue("i", 0);
}


static PyObject* RiverdbPyWriter_write_header(PyObject* self, PyObject* args) {
    PyObject* list;
    if (!PyArg_ParseTuple(args, "O", &list)) {
        std::cout << "parse failed" << std::endl;
        return NULL;
    }
    if (!PyList_Check(list)) {
        std::cout << "param is not list type" << std::endl;
        return Py_BuildValue("i", -1);;
    }

    RiverDB::RiverDBWriter * writer = (RiverDB::RiverDBWriter*)(((RiverdbPyWriter *)self)->writer);

    try {
        int size = PyList_Size(list);
        for (int i = 0; i < size; ++i) {
            PyObject* item = PyList_GetItem(list, i);
            PyObject* name_py = PyTuple_GetItem(item, 0);
            PyObject* type_py = PyTuple_GetItem(item, 1);
            const char* name = PyString_AsString(name_py);
            int type = PyInt_AsLong(type_py);
            writer->push_col_meta(name, type);
        }
        writer->write_header();
    } catch (RiverDB::RTTException& e) {
        std::cout << e.info() << std::endl;
        return Py_BuildValue("i", -1);
    }

    return Py_BuildValue("i", 0);
}

static PyMethodDef WriterSelfMethods[] = {
    {"write_header", (PyCFunction)RiverdbPyWriter_write_header, METH_VARARGS, ""},
    {"write_row", (PyCFunction)RiverdbPyWriter_write_row, METH_VARARGS, ""},
    {"flush", (PyCFunction)RiverdbPyWriter_flush, METH_NOARGS, ""},
    {"close", (PyCFunction)RiverdbPyWriter_close, METH_NOARGS, ""},
    {NULL, NULL, 0, NULL}
};


static PyTypeObject RiverdbWriter_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "riverdb._writer",      /*tp_name*/
    sizeof(RiverdbPyWriter),   /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    RiverdbPyWriter_dealloc,   /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER,
      /* tp_flags: Py_TPFLAGS_HAVE_ITER tells python to
         use tp_iter and tp_iternext fields. */
    "Internal myiter iterator object.",           /* tp_doc */
    0,  /* tp_traverse */
    0,  /* tp_clear */
    0,  /* tp_richcompare naryCSVWriter_write_header*/
    0,  /* tp_weaklistoffset */
    0,  /* tp_iter: __iter__() method */
    0, /* tp_iternext: next() method */
    WriterSelfMethods /* methods */
};

static PyObject * riverdb_writer(PyObject* self, PyObject* args) {
    const char* file;
    if (!PyArg_ParseTuple(args, "s", &file)) {
        return NULL;
    }

    RiverdbPyWriter* binary_writer;
    binary_writer = PyObject_New(RiverdbPyWriter, &RiverdbWriter_Type);
    if (binary_writer == NULL) {
        return NULL;
    }

    if (!PyObject_Init((PyObject*)binary_writer, &RiverdbWriter_Type)) {
        Py_DECREF(binary_writer);
        return NULL;
    }

    RiverDB::RiverDBWriter* writer = new RiverDB::RiverDBWriter(file);
    if (writer == NULL) {
        Py_DECREF(binary_writer);
        return NULL;
    }

    binary_writer->writer = (void *)writer;
    
    return (PyObject *)binary_writer;
}

//static PyObject* time_riverdb(PyObject* self, PyObject* args) {
//    const char* primary_key;
//    const char* index_key;
//    PyObject* load_fpath_list = NULL;
//    const char* append_file;
//    PyObject* col_metas = NULL;
//}

static PyMethodDef RiverdbMethods[] = {
    {"reader", riverdb_reader, METH_VARARGS, ""},
    {"writer", riverdb_writer, METH_VARARGS, ""},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initriverdb_py(void) {
    PyObject* m;
    RiverdbReader_Type.tp_new = PyType_GenericNew; 
    if (PyType_Ready(&RiverdbReader_Type) < 0) {
        return;
    }
    RiverdbWriter_Type.tp_new = PyType_GenericNew; 
    if (PyType_Ready(&RiverdbWriter_Type) < 0) {
        return;
    }
    m = Py_InitModule("riverdb_py", RiverdbMethods);

    Py_INCREF(&RiverdbReader_Type);
    Py_INCREF(&RiverdbWriter_Type);
    PyModule_AddObject(m, "_reader", (PyObject *)&RiverdbReader_Type);
    PyModule_AddObject(m, "_writer", (PyObject *)&RiverdbWriter_Type);
}


