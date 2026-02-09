import 'package:client/models/ai.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';

part 'chat.g.dart';

class AIChatStorage {
  final AIChatId id;
  final List<AIChatMessageItem> messages;
  AIChatState state;

  AIChatStorage({
    required this.id,
    required this.messages,
    required this.state,
  });

  factory AIChatStorage.fromModel(AIChatModel model) {
    return AIChatStorage(
      id: model.id,
      messages: List<AIChatMessageItem>.from(model.messages),
      state: model.state,
    );
  }

  AIChatModel toModel() {
    return AIChatModel(
      id: id,
      messages: List<AIChatMessageItem>.from(messages),
      state: state,
    );
  }
}

class AIChatRepoImpl extends AIChatRepo {
  final Map<AIChatId, AIChatStorage> _aiChats = {};

  AIChatRepoImpl();

  @override
  AIChatListModel getAIChatList() {
    final chats = <AIChatId, AIChatModel>{};
    for (final entry in _aiChats.entries) {
      chats[entry.key] = entry.value.toModel();
    }
    return AIChatListModel(chats: chats);
  }

  @override
  AIChatModel create(AIChatModel model) {
    if (_aiChats.containsKey(model.id)) {
      return _aiChats[model.id]!.toModel();
    }
    _aiChats[model.id] = AIChatStorage.fromModel(model);
    return model;
  }

  @override
  void addMessage(AIChatId id, AIChatMessageItem message) {
    final chat = _aiChats[id];
    if (chat == null) {
      return;
    }
    chat.messages.add(message);
  }

  @override
  void updateMessages(AIChatId id, List<AIChatMessageItem> messages) {
    final chat = _aiChats[id];
    if (chat == null) {
      return;
    }
    chat.messages.clear();
    chat.messages.addAll(messages);
  }

  @override
  void updateState(AIChatId id, AIChatState state) {
    final chat = _aiChats[id];
    if (chat == null) {
      return;
    }
    chat.state = state;
  }

  @override
  void delete(AIChatId id) {
    _aiChats.remove(id);
  }

  @override
  AIChatModel? getAIChatById(AIChatId id) {
    final chat = _aiChats[id];
    return chat?.toModel();
  }

  @override
  void updateMessageById(AIChatId chatId, AIChatMessageId messageId, AIChatMessageItem message) {
    final chat = _aiChats[chatId];
    if (chat == null) {
      return;
    }

    final index = chat.messages.indexWhere((item) {
      return item.maybeWhen(
        userMessage: (msg) => msg.id.value == messageId.value,
        assistantMessage: (msg) => msg.id.value == messageId.value,
        toolsResult: (result) => result.id.value == messageId.value,
        orElse: () => false,
      );
    });

    if (index != -1) {
      chat.messages[index] = message;
    } else {
      chat.messages.add(message);
    }
  }
}

@Riverpod(keepAlive: true)
AIChatRepo aiChatRepo(Ref ref) {
  return AIChatRepoImpl();
}
