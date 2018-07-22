package edu.scu.corpmap;

/**
 * A Camel Application
 */
public class MainApp {

    /**
     * A main() so we can easily run these routing rules in our IDE
     */
    public static void main(String... args) throws Exception {
        IndexTools indexTools = IndexTools.getInstance();
        indexTools.createFullTextIndex();
        indexTools.createSchemaIndex();//对“统一社会信用代码”建立模式索引
        CorpController corpController = CorpController.getCorpController();//这句话的位置不能更改
        corpController.calGraphController();
        System.out.println("结束");
    }

}
