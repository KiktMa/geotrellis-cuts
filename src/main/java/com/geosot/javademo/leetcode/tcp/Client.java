package com.geosot.javademo.leetcode.tcp;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;

/**
 * TCP通信协议，可靠的，效率低的传输方式，三次握手四次挥手
 * 三次握手：
 * 客户端--》服务端：向服务端发送连接请求（可以发送消息）
 * 服务端--》客户端：向客户端发送消息（可接受到消息，且可发送消息）
 * 客户端--》服务端：发送正式连接请求（可以接受到消息）
 * 四次挥手：
 * 客户端--》服务端：发送断开连接请求
 * 服务端--》客户端：接收到请求后但还有数据没有处理完，需要等待处理完成
 * 服务端--》客户端：处理完成，发送消息给客户端
 * 客户端--》服务端：正式断开连接
 */
public class Client {
    public static void main(String[] args) throws Exception {
        // TCP协议实现网络通信,客户端的Socket类,服务器端的ServerSocket类
        Socket socket = new Socket("localhost", 7070);

        // 打开连接到Socket的输入/输出流
        OutputStream outputStream = socket.getOutputStream();
        // 把输出流包装成数据输出流，更方便一些
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        Scanner scanner = new Scanner(System.in);
        while (true){
            System.out.println("请输入：");
            String line = scanner.nextLine();
            if("exit".equals(line)){
                System.out.println("退出成功");
                dataOutputStream.close();
                socket.close();
                break;
            }
            // 将输入的内容转换为字节输出流
            dataOutputStream.writeUTF(line);
            // 这里必须要刷新让数据发送出去
            dataOutputStream.flush();
        }
    }
}
