import 'dart:convert';

import 'package:db_driver/db_driver.dart';
import 'package:freezed_annotation/freezed_annotation.dart';
import 'package:client/utils/state_value.dart';
import 'package:uuid/uuid.dart';

part 'ai.freezed.dart';
part 'ai.g.dart';

abstract class LLMAgentRepo {
  LLMAgentsModel getLLMAgents();
  LLMAgentModel? getLastUsedLLMAgent();
  void updateLastUsedLLMAgent(LLMAgentId id);
  void create(LLMAgentSettingModel setting);
  void delete(LLMAgentId id);
  LLMAgentModel? getLLMAgentById(LLMAgentId id);
  void updateStatus(LLMAgentId id, LLMAgentStatusModel status);
  void updateSetting(LLMAgentId id, LLMAgentSettingModel setting);
}

abstract class AIChatRepo {
  AIChatListModel getAIChatList();
  AIChatModel create(AIChatModel model);
  AIChatModel? getAIChatById(AIChatId id);
  void delete(AIChatId id);
  void updateMessages(AIChatId id, List<AIChatMessageItem> messages);
  void addMessage(AIChatId id, AIChatMessageItem message);
  void updateState(AIChatId id, AIChatState state);
  void updateMessageById(AIChatId chatId, AIChatMessageId messageId, AIChatMessageItem message);
}

enum LLMAgentState {
  unknown,
  testing,
  available,
  unavailable,
}

@freezed
abstract class LLMAgentId with _$LLMAgentId {
  const factory LLMAgentId({
    required int value,
  }) = _LLMAgentId;
}

@freezed
abstract class LLMAgentSettingModel with _$LLMAgentSettingModel {
  const factory LLMAgentSettingModel({
    required String name,
    required String baseUrl,
    required String apiKey,
    required String modelName,
  }) = _LLMAgentSettingModel;
}

@freezed
abstract class LLMAgentStatusModel with _$LLMAgentStatusModel {
  const factory LLMAgentStatusModel({
    required LLMAgentState state,
    String? error,
  }) = _LLMAgentStatusModel;
}

@freezed
abstract class LLMAgentModel with _$LLMAgentModel {
  const factory LLMAgentModel({
    required LLMAgentId id,
    required LLMAgentSettingModel setting,
    required LLMAgentStatusModel status,
  }) = _LLMAgentModel;
}

@freezed
abstract class LLMAgentsModel with _$LLMAgentsModel {
  const factory LLMAgentsModel({
    required Map<LLMAgentId, LLMAgentModel> agents,
    required LLMAgentModel? lastUsedLLMAgent,
  }) = _LLMAgentsModel;
}

enum AIRole {
  user,
  assistant,
}

enum AIChatState {
  idle,
  waiting,
  error,
}

@freezed
abstract class AIChatId with _$AIChatId {
  const factory AIChatId({
    required int value,
  }) = _AIChatId;
}

@freezed
abstract class AIChatMessageId with _$AIChatMessageId {
  const factory AIChatMessageId({
    required String value,
  }) = _AIChatMessageId;

  /// 创建一个新的 AIChatMessageId，自动生成 UUID
  factory AIChatMessageId.generate() => AIChatMessageId(value: const Uuid().v4());
}

@freezed
abstract class AIChatModel with _$AIChatModel {
  const factory AIChatModel({
    required AIChatId id,
    required List<AIChatMessageItem> messages,
    required AIChatState state,
  }) = _AIChatModel;
}

// user message
@freezed
abstract class AIChatUserMessageModel with _$AIChatUserMessageModel {
  const AIChatUserMessageModel._();

  const factory AIChatUserMessageModel({
    required AIChatMessageId id,
    required String content,
    String? ref,
  }) = _AIChatUserMessageModel;

  String toMessage() {
    final refText = ref?.trim() ?? '';
    if (refText.isEmpty) return content;
    // ref 作为“额外上下文”，拼到 user message 里供 LLM 使用，但 UI 仍可只展示 content。
    return '$content\n\nref:\n$refText';
  }
}

@freezed
abstract class AIChatAssistantMessageModel with _$AIChatAssistantMessageModel {
  const AIChatAssistantMessageModel._();

  const factory AIChatAssistantMessageModel({
    required AIChatMessageId id,
    required String content,
    String? thinking, // reason_content from OpenAI API
    String? error,
    @Default(State.running) State status,
  }) = _AIChatAssistantMessageModel;

  String toMessage() {
    if (thinking != null && thinking!.isNotEmpty) {
      return '<think>\n$thinking\n</think>\n$content';
    }
    return content;
  }

  /// 判断思考是否结束
  /// 当 content 有值或者状态为完成时，思考结束
  bool get isThinkingCompleted {
    return content.isNotEmpty || status == State.done;
  }
}

@freezed
abstract class AIChatMessageToolCallQueryModel with _$AIChatMessageToolCallQueryModel {
  const factory AIChatMessageToolCallQueryModel({
    required String query,
    StateValue<BaseQueryResult>? result,
    Duration? executeTime,
  }) = _AIChatMessageToolCallQueryModel;
}

@freezed
abstract class AIChatMessageToolCallsModel with _$AIChatMessageToolCallsModel {
  const AIChatMessageToolCallsModel._();

  const factory AIChatMessageToolCallsModel({
    required AIChatMessageId id,
    required AIChatMessageToolCallQueryModel toolCall,
  }) = _AIChatMessageToolCallsModel;

  String toMessage() {
    if (toolCall.result == null) return '';
    return toolCall.result!.match(
      (result) => _getSQLResultString(result) ?? '',
      (error) => error,
      () => '正在执行查询...',
    );
  }

  /// 获取 SQL Result 字符串
  ///
  /// [result] SQL 查询结果
  ///
  /// 返回 JSON 字符串，包含查询结果的列信息和数据行
  String? _getSQLResultString(BaseQueryResult result) {
    try {
      final data = <String, dynamic>{
        'success': true,
        'affectedRows': result.affectedRows.toString(),
        'columns': result.columns
            .map((c) => {
                  'name': c.name,
                  'type': c.dataType().name,
                })
            .toList(),
        'rows': result.rows.map((row) {
          final rowMap = <String, dynamic>{};
          for (var i = 0; i < row.values.length && i < result.columns.length; i++) {
            final column = result.columns[i];
            final value = row.values[i];
            rowMap[column.name] = value.getString();
          }
          return rowMap;
        }).toList(),
      };
      final jsonString = jsonEncode(data);
      return jsonString;
    } catch (e) {
      return jsonEncode({
        'success': false,
        'error': e.toString(),
      });
    }
  }
}

/// 消息项联合类型，可以存储消息或工具调用结果
@freezed
abstract class AIChatMessageItem with _$AIChatMessageItem {
  const factory AIChatMessageItem.userMessage(AIChatUserMessageModel message) = _AIChatMessageItemUserMessage;
  const factory AIChatMessageItem.assistantMessage(AIChatAssistantMessageModel message) =
      _AIChatMessageItemAssistantMessage;
  const factory AIChatMessageItem.toolsResult(AIChatMessageToolCallsModel toolsResult) = _AIChatMessageItemToolResult;
}

@freezed
abstract class AIChatListModel with _$AIChatListModel {
  const factory AIChatListModel({
    required Map<AIChatId, AIChatModel> chats,
  }) = _AIChatListModel;
}

/// AI生成的文件名和描述结果
@freezed
abstract class ExportFileNameResult with _$ExportFileNameResult {
  const factory ExportFileNameResult({
    required String fileName,
    String? desc,
  }) = _ExportFileNameResult;

  factory ExportFileNameResult.fromJson(Map<String, dynamic> json) => _$ExportFileNameResultFromJson(json);
}
