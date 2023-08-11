package com.geosot.javademo.leetcode.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Scanner;

/**
 * UDP协议，不可靠的，高效的传输方式，由客户端将信息一次性打包发送到服务端
 * 不管发送失败或数据损失，可以常用于语音和视频等信息传世
 */
public class SendServer {

    // UDP客户端
    public static void main(String[] args) throws Exception {

        DatagramSocket socket = new DatagramSocket(7777);
        Scanner sc = new Scanner(System.in);
        while(true){
            System.out.println("请说：");
            String words = sc.nextLine();
            if ("exit".equals(words)) {
                System.out.println("退出成功");
                socket.close();
                break;
            }
            byte[] wordsBytes = words.getBytes();
            DatagramPacket packet = new DatagramPacket(wordsBytes,0,wordsBytes.length,
                    InetAddress.getLocalHost(),8888);
            // 发送数据
            socket.send(packet);
        }
    }
}
