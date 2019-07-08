package nio;

import core.message.UserMessageProto;

public class Server extends NioServer {
    @Override
    public MessageProcess getInstance() {
        return new MessageProcessImpl();
    }

    private class MessageProcessImpl extends MessageProcess {
        @Override
        public void run() {
            while (!isShutdown) {
                Object task = null;
                try {
                    task = readCache.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (task instanceof TaskEntity) {
                    //完整的数据,处理对应流程
                    TaskEntity taskEntity = (TaskEntity) task;
                    UserMessageProto.UserMessage message = UserMessageProto.UserMessage.newBuilder()
                            .setMessageType(UserMessageProto.MessageType.RESPONSE)
                            .setResponseMessage(UserMessageProto.ResponseMessage
                                    .newBuilder()
                                    .setVal("test")
                                    .setResponseType(UserMessageProto.ResponseType.SUCCESS)
                                    .build()
                            ).build();
                    taskEntity.setMsg(message);
                    nioWriteGroup.addTask(taskEntity);
                } else {
                    nioWriteGroup.addTask(object);
                }
            }
        }
    }
}
