package com.geosot.javademo.leetcode.tcp;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    public static void main(String[] args) throws Exception {
        // TCP协议实现网络通信,客户端的Socket类,服务器端的ServerSocket类，
        // 创建ServerSocket对象，绑定监听端口7070
        ServerSocket serverSocket = new ServerSocket(7070);

        // 监听客户端请求
        Socket socket = serverSocket.accept();

        // 将客户端发送的信息转换成字节输入流
        InputStream inputStream = socket.getInputStream();
        // 将字节输入流包装成数据输入流，更好的接收客户端传过来的数据
        DataInputStream dataInputStream = new DataInputStream(inputStream);

        while (true){
            try {
                // 这里客户端用什么方式输入就用什么接收
                // 客户端使用writeUTF，这里读数据就用readUTF
                String res = dataInputStream.readUTF();
                System.out.println(res);
            } catch (IOException e) {
                System.out.println(socket.getRemoteSocketAddress()+" 下线了");
                dataInputStream.close();
                serverSocket.close();
            }
        }
    }
}
