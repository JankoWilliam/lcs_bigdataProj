package cn.yintech.recommend;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.*;
import org.ansj.library.DicLibrary;
import org.ansj.util.MyStaticValue;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class AnsjWordSplitTest {

    public static void main(String[] args) throws IOException {

//        String str = "在ZOL全平台评论苹果发布会直播，评论内容需和苹果相关即可，截止到9月16日12时，选取评论数的15%、35%、65%、85%的用户，每人获得1MORE 舒适豆耳机。无意义刷楼、注册小号刷楼、严重刷屏将取消获奖资格，最终奖品解释权归ZOL所有！";
//        String str = "苹果发布会直播";
//        System.out.println("BaseAnalysis:" + BaseAnalysis.parse(str));
//        System.out.println("ToAnalysis:" + ToAnalysis.parse(str));
//        System.out.println("DicAnalysis:" + DicAnalysis.parse(str));
//        System.out.println("IndexAnalysis:"+IndexAnalysis.parse(str));
//        System.out.println("NlpAnalysis:"+NlpAnalysis.parse(str));


//        final Result parse = IndexAnalysis.parse("苹果发布会直播");
//        System.out.println(parse);

        // 关闭名字识别
        MyStaticValue.isNameRecognition = false;
        // 配置自定义词典的位置。注意是绝对路径
        MyStaticValue.ENV.put(DicLibrary.DEFAULT, AnsjWordSplitTest.class.getClassLoader().getResource("default.dic").getPath());

        // 增加新词,中间按照'\t'隔开
        DicLibrary.insert(DicLibrary.DEFAULT, "苹果发布会");
        final Result parse = NlpAnalysis.parse("我觉得Ansj中文分词是一个不错的系统!我是王婆!在ZOL全平台评论苹果发布会直播，评论内容需和苹果相关即可，截止到9月16日12时，选取评论数的15%、35%、65%、85%的用户，每人获得1MORE 舒适豆耳机。无意义刷楼、注册小号刷楼、严重刷屏将取消获奖资格，最终奖品解释权归ZOL所有！");
        final Result parse2 = IndexAnalysis.parse("我觉得Ansj中文分词是一个不错的系统!我是王婆!在ZOL全平台评论苹果发布会直播，评论内容需和苹果相关即可，截止到9月16日12时，选取评论数的15%、35%、65%、85%的用户，每人获得1MORE 舒适豆耳机。无意义刷楼、注册小号刷楼、严重刷屏将取消获奖资格，最终奖品解释权归ZOL所有！");
        System.out.println("增加新词例子:" + parse);
        System.out.println("增加新词例子:" + parse2);
        // 删除词语,只能删除.用户自定义的词典.
//        UserDefineLibrary.removeWord("ansj中文分词");
//        terms = ToAnalysis.parse("我觉得ansj中文分词是一个不错的系统!我是王婆!");
//        System.out.println("删除用户自定义词典例子:" + terms);

    }

}
