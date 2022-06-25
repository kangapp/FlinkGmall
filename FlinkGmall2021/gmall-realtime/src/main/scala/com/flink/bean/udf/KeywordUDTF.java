package com.flink.bean.udf;

import com.flink.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row>{

    public void eval(String str) {
        try {
            List<String> words = KeywordUtil.analyze(str);

            for (String word : words) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }
    }
}
