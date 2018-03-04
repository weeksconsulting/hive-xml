package org.apache.hadoop.hive.ql.io.xml;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractXmlReader {

    public static final String EMPTY_STRING = "";
    public static final Map<String,String> EMPTY_NAMESPACE_LIST = new HashMap<>();

    abstract void initialize(InputStream is, Map<String, String> namespaces, String rootXPath, List<String> colXPaths) throws IOException;

    abstract boolean next();

    abstract int size();

    abstract String getString(int i) throws IOException;
}
