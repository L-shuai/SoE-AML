package com.soe.utils;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CsvUtil {

    /**
     * CSV文件生成方法  字符流追加：FileWriter writer = new FileWriter(file，true)
     *
     * @param headLabel 头部标签（标题行，每个文件只写入第一行，若已存在，则不用再插入标题行）
     * @param dataList  数据列表
     * @param filePath  文件路径
     * @param addFlag   是否追加
     */
    public static void writeToCsv(String headLabel, List<String> dataList, String filePath, boolean addFlag) {
        BufferedWriter buffWriter = null;
        try {
            //根据指定路径构建文件对象
            File csvFile = new File(filePath);
            //文件输出流对象，第二个参数时boolean类型,为true表示文件追加（在已有的文件中追加内容）
//            FileWriter writer = new FileWriter(csvFile, addFlag);
            //构建缓存字符输出流（不推荐使用OutputStreamWriter）
//            buffWriter = new BufferedWriter(writer, 1024);
//            buffWriter = new BufferedWriter(new OutputStreamWriter (new FileOutputStream (filePath,addFlag),"UTF-8"));;
            buffWriter = new BufferedWriter(new OutputStreamWriter (new FileOutputStream (filePath,addFlag),"UTF-8"));

//            buffWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), "utf-8"));
            // 由于csv是UTF-8格式，用Excel打开就会乱码，所以加个BOM告诉Excel用UTF-8打开
//            String csv = new String(new byte[] { (byte) 0xEF, (byte) 0xBB,(byte) 0xBF });
//            buffWriter.write(csv);
            //头部不为空则写入头部，并且换行
            if (StringUtils.isNotBlank(headLabel)) {
                FileReader fileReader = null;
                try {
                    fileReader = new FileReader(csvFile);
                    //构建缓存字符输入流
                    BufferedReader buffReader = new BufferedReader(fileReader);
                    String line = "";
                    //根据合适的换行符来读取一行数据,赋值给line
                    if ((line = buffReader.readLine()) == null) {
                        //如果为空文件，则插入标题
                        buffWriter.write(headLabel);
                        buffWriter.newLine();
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }


            }
                //遍历list
            for (String rowStr : dataList) {
                //如果数据不为空，则写入文件内容,并且换行
                if (StringUtils.isNotBlank(rowStr)) {
                    buffWriter.write(rowStr);
                    buffWriter.newLine();//文件写完最后一个换行不用处理
                }
            }
            //刷新流，也就是把缓存中剩余的内容输出到文件
            buffWriter.flush();
        } catch (Exception e) {
            System.out.println("写入csv出现异常");
            e.printStackTrace();
        } finally {
            try {
                //关闭流
                if (buffWriter != null) {
                    buffWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 根据文件路径读取csv文件的内容
     *
     * @param filePath
     * @return
     */
    public static List<String> readFromCsv(String filePath) {
        ArrayList<String> dataList = new ArrayList<>();
        BufferedReader buffReader = null;
        try {
            //构建文件对象
            File csvFile = new File(filePath);
            //判断文件是否存在
            if (!csvFile.exists()) {
                System.out.println("文件不存在");
                return dataList;
            }
            //构建字符输入流
            FileReader fileReader = new FileReader(csvFile);
            //构建缓存字符输入流
            buffReader = new BufferedReader(fileReader);
            String line = "";
            //根据合适的换行符来读取一行数据,赋值给line
            while ((line = buffReader.readLine()) != null) {
                if (StringUtils.isNotBlank(line)) {
                    //数据不为空则加入列表
                    dataList.add(line);
                }
            }
        } catch (Exception e) {
            System.out.println("读取csv文件发生异常");
            e.printStackTrace();
        } finally {
            try {
                //关闭流
                if (buffReader != null) {
                    buffReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return dataList;
    }


    @Test
    public void writeToCsv() {
        String headDataStr = "规则代码,预警日期,客户号,客户名称,折人民币交易金额-收,折人民币交易金额-付,交易笔数收,交易笔数付";
        String csvfile = "./result/test.csv";
        List<String> dataList = new ArrayList<>();
        dataList.add("JRSJ-001,2021-01-19,9999980070068446,李佳琪,100000,600000,1,4");
        dataList.add("JRSJ-001,2021-01-19,9999980070068446,李佳琪,100000,600000,1,4");
        dataList.add("JRSJ-001,2021-01-19,9999980070068446,李佳琪,100000,600000,1,9");
        CsvUtil.writeToCsv(headDataStr, dataList, csvfile, true);
    }

    @Test
    public void readCsv() {
        String headDataStr = "规则代码,预警日期,客户号,客户名称,折人民币交易金额-收,折人民币交易金额-付,交易笔数收,交易笔数付";
        String csvfile = "./result/result.csv";
        System.out.println(readFromCsv(csvfile));
    }

}
