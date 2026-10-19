package df.digitalfix_ms_admin_queues.service;

import df.digitalfix_ms_admin_queues.dto.ListenerContainerDTO;

import java.util.List;

public interface ListenerAdminService {

    List<ListenerContainerDTO> getAllListeners();
    ListenerContainerDTO getListenerById(String listenerId);
    void startListener(String listenerId);
    void stopListener(String listenerId);
    void pauseListener(String listenerId);
    void resumeListener(String listenerId);
}
