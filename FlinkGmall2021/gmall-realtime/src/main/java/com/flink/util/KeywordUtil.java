package com.flink.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    //使用 IK 分词器对字符串进行分词
    public static List<String> analyze(String text) throws IOException {
        StringReader sr = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(sr, true);
        Lexeme lex = null;
        List<String> keywordList = new ArrayList();
        while (true) {
            if ((lex = ik.next()) != null) {
                String lexemeText = lex.getLexemeText();
                keywordList.add(lexemeText);
            } else {
                break;
            }
        }
        return keywordList;
    }
    public static void main(String[] args) throws IOException {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信 4G 手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));
    }
}
