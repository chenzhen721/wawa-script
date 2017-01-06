
@Grab("org.java-websocket:Java-WebSocket:1.3.0")
import org.java_websocket.WebSocketImpl
import org.java_websocket.client.WebSocketClient
import org.java_websocket.drafts.Draft_10
import org.java_websocket.handshake.ServerHandshake

/**
 * date: 13-8-12 下午4:57
 * @author: yangyang.cong@ttpod.com
 */
class WSClient  {

    static final  String uri = "ws://test.ws.dongting.com:8080/primus";
//    static void javax_ws(){
//        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
//
//        System.out.println("Connecting to " + uri);
//        try {
//            Session session = container.connectToServer(new Endpoint() {
//                void onOpen(Session session, EndpointConfig config) {
//
//                }
//                @OnMessage
//                public void processMessageFromServer(String message, Session session) {
//                    System.out.println("Message came from the server ! " + message);
//                }
//            }, URI.create(uri));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }



    static void main(args){

        WebSocketImpl.DEBUG = true
        int conn_time_out_seconds = 30
        def client =  new WebSocketClient(URI.create(uri),new Draft_10(),
                [room_id:"1024036",access_token:null],conn_time_out_seconds){
            void onOpen(ServerHandshake handshakedata) {
            }

            void onMessage(String message) {
                println "[ Rec JSON ] ->  ${message}"
            }

            void onClose(int code, String reason, boolean remote) {
            }

            void onError(Exception ex) {
            }
        };
        client.connectBlocking()
        client.send("Hello..")
    }
}
