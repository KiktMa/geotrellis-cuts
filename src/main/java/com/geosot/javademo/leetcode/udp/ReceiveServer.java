package com.geosot.javademo.leetcode.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class ReceiveServer {

    public static void main(String[] args) throws Exception {
        System.out.println("====UDP服务端启动====");
        DatagramSocket socket = new DatagramSocket(8888);

        byte[] bytes = new byte[1024 * 64]; // 64kb
        DatagramPacket packet = new DatagramPacket(bytes,0,bytes.length);
        while(true){
            socket.receive(packet);

            int length = packet.getLength();

            String res = new String(bytes, 0, length);
            System.out.println(res);
            System.out.println(packet.getAddress().getHostAddress());
            System.out.println(packet.getPort());
        }
    }
}
