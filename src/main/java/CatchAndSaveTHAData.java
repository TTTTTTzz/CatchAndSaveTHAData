import db.DBUtil;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class CatchAndSaveTHAData {
    private final static String imageURL = "https://weather.tmd.go.th/composite/compositeQPE_VTBB_latest.png";
    private final static String zipURL = "https://weather.tmd.go.th/composite/compositeQPE_VTBB_latest.asc.zip";
    private final static String configPath = "./DownloadConfiguration.txt";
    private static String downloadDirectory = "";
    private static String fileName = "";

    public static void main(String[] args) {
        //如果没有参数，则运行抓取数据并保存的程序，否则根据参数运行查询的程序
        if (args.length == 0) {
            Timer timer = new Timer();
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    //读取配置文件，设置下载目录
                    setDownloadDirectory();
                    //下载zip文件
                    downloadZip();
                    Timestamp downloadTime = new Timestamp(new Date().getTime());
                    //解压zip文件，重命名数据文件
                    if(unzip()){
                        //下载图片，重命名图片
                        downloadImage();
                        //数据入库
                        try {
                            String currentFilePath = downloadDirectory + "\\" + fileName;
                            File currentFile = new File(currentFilePath);
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                            Date dataTimeTemp = sdf.parse(fileName);
                            Timestamp dataTime = new Timestamp(dataTimeTemp.getTime());

                            DBUtil.insert(currentFile.getName(), currentFile.length(), downloadTime, dataTime);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            };

            //如果当前时间接近整点35分，则运行task
            Calendar cal = Calendar.getInstance();
            int currentMin = cal.get(Calendar.MINUTE);
            if (Math.abs(currentMin - 35) < 5) {
                task.run();
            }

            //todo 检查是否任何时候都能进入定时器的设置 35
            if ((15 < currentMin || 15 == currentMin) && currentMin < 35) {
                cal.set(Calendar.MINUTE, 35);
                //在设定的时间开始，间隔为20分钟
                timer.schedule(task, cal.getTime(), 1000 * 60 * 20);
                System.out.println("设定定时下载将在" + cal.getTime() + "开始");
            } else if ((35 < currentMin || 35 == currentMin) && currentMin < 55) {
                cal.set(Calendar.MINUTE, 55);
                timer.schedule(task, cal.getTime(), 1000 * 60 * 20);
                System.out.println("设定定时下载将在" + cal.getTime() + "开始");
            } else if (currentMin < 15) {
                cal.set(Calendar.MINUTE, 15);
                timer.schedule(task, cal.getTime(), 1000 * 60 * 20);
                System.out.println("设定定时下载将在" + cal.getTime() + "开始");
            } else if (55 < currentMin) {
                cal.set(Calendar.MINUTE, 15);
                cal.add(Calendar.HOUR, 1);
                timer.schedule(task, cal.getTime(), 1000 * 60 * 20);
                System.out.println("设定定时下载将在" + cal.getTime() + "开始");
            }
        } else {
            //根据参数运行查询的程序
            setDownloadDirectory();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat();// 格式化时间
            simpleDateFormat.applyPattern("yyyyMMddHHmmss");
            Date date = new Date();// 获取当前时间

            String outputPath = downloadDirectory + "\\" + simpleDateFormat.format(date);
            switch (args[0]) {
                case "1":
                    outputPath = outputPath + "最新文件的数据时间.csv";
                    DBUtil.selectLastest(outputPath);
                    break;
                case "2":
                    outputPath = outputPath + "按天统计下载的文件.csv";
                    DBUtil.statisticsByDay(outputPath);
                    break;
                case "3":
                    outputPath = outputPath + "按小时统计下载的文件.csv";
                    DBUtil.statisticsByHour(outputPath);
                    break;
                case "4":
                    outputPath = outputPath + "按天检查下载的文件.csv";
                    DBUtil.statisticsByHourAndCheck(outputPath);
                    break;
                case "5":
                    outputPath = outputPath + "文件数据时间查询结果.csv";
                    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in))) {
                        System.out.println("请输入查询的文件数据时间，格式为yyyy-mm-dd HH");
                        String input = bufferedReader.readLine();
                        String[] inputArray = input.split("\\s+");
                        DBUtil.selectAndPrintOne(outputPath, inputArray[0] + " " + inputArray[1]);
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                    break;
            }
        }
    }

    private static void setDownloadDirectory() {
        System.out.println("正在读取配置文件...");
        try {
            InputStream inputStream = new FileInputStream(configPath);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String str = bufferedReader.readLine();

            if (str == null) { //判断配置文件是否为空
                throw new Exception("未在DownloadConfiguration.txt中设置目录！");
            } else if (!new File(str).isDirectory()) { //判断目录是否存在且是否合法
                throw new Exception("目录不合法或目录不存在！");
            }

            downloadDirectory = str;
            System.out.println("当前下载目录为：" + downloadDirectory);

        } catch (Exception e) {
            e.printStackTrace();
            printError(e);
            System.exit(1);
        }
    }

    private static void downloadZip() {
        try {
            System.out.println("开始下载zip文件");
            URL url = new URL(zipURL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            InputStream in = connection.getInputStream();
            FileOutputStream out = new FileOutputStream(downloadDirectory + "\\" + "download.zip");
            //读取输入流，写入输出流
            copy(in, out);
            out.close();
            in.close();
            System.out.println("下载zip文件成功");
        } catch (IOException e) {
            e.printStackTrace();
            printError(e);
            System.exit(1);
        }
    }

    private static void downloadImage() {
        try {
            System.out.println("开始下载png图片");
            URL url = new URL(imageURL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            InputStream in = connection.getInputStream();
            FileOutputStream out = new FileOutputStream(new File(downloadDirectory + "\\" + fileName.substring(0, fileName.indexOf('.')) + ".png"));//new FileOutputStream(fileName.substring(0,fileName.indexOf('.')) + ".png");
            //读取输入流，写入输出流
            copy(in, out);
            out.close();
            in.close();
            System.out.println("下载图片成功");
        } catch (IOException e) {
            e.printStackTrace();
            printError(e);
            System.exit(1);
        }
    }

    //如果数据未更新返回false，否则返回true
    private static boolean unzip() {
        try {
            String zipPath = downloadDirectory + "\\download.zip";
            File file = new File(zipPath);
            ZipFile zipFile = new ZipFile(file);
            ZipEntry zipEntry = zipFile.entries().nextElement();
            String zipEntryName = zipEntry.getName();

            InputStream in = zipFile.getInputStream(zipEntry);
            //截取文件名中的数据时间作为新的文件名
            fileName = zipEntryName.substring(10, 24) + zipEntryName.substring(zipEntryName.indexOf('.'));

            //如果下载目录中已存在同名数据，则表示数据未更新，退出程序
            if (new File(downloadDirectory + "\\" + fileName).exists()) {
                System.out.println("数据未更新！");
                return false;
            } else {
                //解压zip文件
                String outPath = (downloadDirectory + "\\" + fileName).replace("/", File.separator);
                System.out.println("当前zip解压之后的路径为：" + outPath);
                OutputStream out = new FileOutputStream(outPath);
                copy(in, out);
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            printError(e);
            System.exit(1);
        }
        return false;
    }

    private static void copy(InputStream input, OutputStream output) {
        try {
            byte[] buf = new byte[1024];
            int n = input.read(buf);
            while (n >= 0) {
                output.write(buf, 0, n);
                n = input.read(buf);
            }
            output.flush();
        } catch (IOException e) {
            e.printStackTrace();
            printError(e);
            System.exit(1);
        }
    }

    private static void printError(Exception e) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat();// 格式化时间
            simpleDateFormat.applyPattern("yyyyMMddHHmmss");
            Date date = new Date();// 获取当前时间

            String outputPath;
            if (!new File(downloadDirectory).isDirectory()) {
                outputPath = "./" + simpleDateFormat.format(date) + "错误提示.txt";
            } else {
                outputPath = downloadDirectory + "/" + simpleDateFormat.format(date) + "错误提示.txt";
            }
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath, true), "GB2312"));
            writer.write(e.getMessage());
            writer.flush();
            writer.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    private void insertDataFromDir() throws ParseException {
        String path = "C:\\Users\\ttttt\\Desktop\\data";
        File dir = new File(path);
        String[] fileList = dir.list();
        for (String currentFileName : fileList) {
            String currentPath = path + "\\" + currentFileName;
            File currentFile = new File(currentPath);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            Date dataTimeTemp = sdf.parse(currentFileName);
            Timestamp dataTime = new Timestamp(dataTimeTemp.getTime());
            Timestamp downloadTime = new Timestamp(currentFile.lastModified());
            DBUtil.insert(currentFileName, currentFile.length(), downloadTime, dataTime);
        }
    }
}
