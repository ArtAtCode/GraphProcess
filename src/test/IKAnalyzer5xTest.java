import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;
import java.io.StringReader;

public class IKAnalyzer5xTest {
    public static void main(String[] args) throws IOException {
        Analyzer analyzer = new IKAnalyzer5x(true);
        TokenStream ts = analyzer.tokenStream("field",
                new StringReader(
                        "IK Analyzer 是一个开源的，基于java语言开发的轻量级的中文分词工具包。" +
                                "从2006年12月推出1.0版开始， IKAnalyzer已经推出了4个大版本。" +
                                "最初，它是以开源项目Luence为应用主体的，" +
                                "结合词典分词和文法分析算法的中文分词组件。从3.0版本开始，" +
                                "IK发展为面向Java的公用分词组件，独立于Lucene项目，" +
                                "同时提供了对Lucene的默认优化实现。在2012版本中，" +
                                "IK实现了简单的分词歧义排除算法，" +
                                "标志着IK分词器从单纯的词典分词向模拟语义分词衍化。"));

        OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
        try {
            ts.reset();
            while (ts.incrementToken()) {
                System.out.println(offsetAtt.toString());
            }
            ts.end();
        } finally {
            ts.close();
        }
    }
}
