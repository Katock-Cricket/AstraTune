import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import time
from utils.timing import log_timing, sql_kind
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.live import Live
from rich.table import Table
from rich.text import Text
from rich import box


class StreamHandler:
    """
    流式事件处理器
    
    支持三种输出模式：
    1. rich: Rich 美化终端显示（流式模式）
    2. logger: Logger 输出（非流式模式）
    3. structured: 结构化字典输出（为 Gradio 预留）
    """
    
    def __init__(
        self,
        mode: str = "rich",
        logger: Optional[logging.Logger] = None
        ):
        """
        初始化流式事件处理器
        
        Args:
            mode: 输出模式 ("rich", "logger", "structured")
            logger: Logger 实例（用于文件记录）
            verbose: 是否输出详细信息
        """
        self.mode = mode
        self.logger = logger
        
        # Rich 控制台
        if mode == "rich":
            self.console = Console()
        
        # 状态追踪
        self.current_iteration = 0
        self.llm_accumulated_text = ""
        self.tool_call_count = 0
        self.events_history: List[Dict[str, Any]] = []
        self._llm_t0: Optional[float] = None
        self._tool_t0: Optional[float] = None
        self._tool_name: str = ""
        
        # 事件类型映射
        self.event_type_names = {
            "on_chain_start": "[节点开始]",
            "on_chain_end": "[节点结束]",
            "on_chat_model_start": "[LLM开始]",
            "on_chat_model_stream": "[LLM输出]",
            "on_chat_model_end": "[LLM完成]",
            "on_tool_start": "[工具调用开始]",
            "on_tool_end": "[工具调用完成]",
        }
    
    def handle_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单个事件
        
        Args:
            event: LangGraph 事件
            
        Returns:
            结构化事件（structured 模式），否则返回 None
        """
        event_type = event.get("event")
        self._note_timing(event)

        # 记录事件历史
        self.events_history.append(event)
        
        # # 同时记录到 logger
        # if self.logger:
        #     self._log_to_file(event)
        
        # 根据模式处理
        if self.mode == "rich":
            self._handle_rich(event)
        elif self.mode == "logger":
            self._handle_logger(event)
        elif self.mode == "structured":
            return self._handle_structured(event)
        
        return None
    
    def _handle_rich(self, event: Dict[str, Any]) -> None:
        """Rich 美化显示"""
        event_type = event.get("event")
        event_name = event.get("name", "")
        event_data = event.get("data", {})
        
        # 节点开始
        if event_type == "on_chain_start":
            if "reasoning" in event_name:
                self.current_iteration += 1
                self.console.print(Panel(
                    f"[bold cyan]迭代 {self.current_iteration}[/bold cyan]",
                    title="🔍 推理节点",
                    border_style="cyan"
                ))
            elif "force_conclusion" in event_name:
                self.console.print(Panel(
                    "[bold yellow]达到最大迭代次数，强制生成结论[/bold yellow]",
                    title="⚠️ 强制结论节点",
                    border_style="yellow"
                ))
            elif "tools" in event_name:
                pass  # 工具节点开始，等待具体工具调用
        
        # 节点结束
        elif event_type == "on_chain_end":
            if "reasoning" in event_name:
                self.console.print(f"[dim]└─ 推理节点结束[/dim]\n")
        
        # LLM 开始
        elif event_type == "on_chat_model_start":
            self.console.print("[bold blue]🤖 LLM 开始生成...[/bold blue]")
            self.llm_accumulated_text = ""
        
        # LLM Token 流
        elif event_type == "on_chat_model_stream" or event_type == "on_chain_stream":
            chunk = event_data.get("chunk")
            if hasattr(chunk, "content") and chunk.content:
                token = chunk.content
                self.llm_accumulated_text += token
                # 实时打印 token
                self.console.print(token, end="", style="bold white")
        
        # LLM 完成
        elif event_type == "on_chat_model_end":
            if self.llm_accumulated_text:
                self.console.print()  # 换行
                
                # 检查是否有工具调用
                output = event_data.get("output", {})
                if hasattr(output, "tool_calls") and output.tool_calls:
                    self.console.print(f"\n[bold cyan]🔧 LLM 请求调用 {len(output.tool_calls)} 个工具[/bold cyan]")
                elif "【诊断结论】" in self.llm_accumulated_text:
                    self.console.print(Panel(
                        self.llm_accumulated_text,
                        title="🎯 诊断结论",
                        border_style="green",
                        box=box.DOUBLE
                    ))
                self.console.print()
        
        # 工具调用开始
        elif event_type == "on_tool_start":
            self.tool_call_count += 1
            tool_name = event.get("name", "unknown")
            tool_input = event_data.get("input", {})
            
            self.console.print(Panel(
                self._format_tool_input(tool_input),
                title=f"🔧 工具调用 #{self.tool_call_count}: {tool_name}",
                border_style="blue"
            ))
        
        # 工具调用完成
        elif event_type == "on_tool_end":
            tool_name = event.get("name", "unknown")
            output = event_data.get("output", "").content
            
            # 限制输出长度
            output_str = str(output)
            if len(output_str) > 500:
                output_display = output_str[:500] + "\n..."
            else:
                output_display = output_str
            
            self.console.print(Panel(
                output_display,
                title=f"✅ 工具返回: {tool_name}",
                border_style="green"
            ))
            self.console.print()
    
    def _handle_logger(self, event: Dict[str, Any]) -> None:
        """Logger 输出（非流式模式）"""
        if not self.logger:
            return
        
        event_type = event.get("event")
        event_name = event.get("name", "")
        event_data = event.get("data", {})
        
        # 只记录关键事件
        if event_type == "on_chain_start":
            if "reasoning" in event_name:
                self.current_iteration += 1
                self.logger.info(f"推理节点 - 迭代 {self.current_iteration}")
            elif "force_conclusion" in event_name:
                self.logger.info("强制结论节点")

        elif event_type == "on_chain_end":
            if "reasoning" in event_name:
                try:
                    msgs = (event_data.get("output") or {}).get("messages") or []
                    last = msgs[-1] if msgs else None
                    text = getattr(last, "content", "") if last is not None else ""
                    if isinstance(text, str) and len(text) > 800:
                        text = text[:800] + "…"
                    if text:
                        self.logger.info(text)
                except Exception:
                    pass
            elif "force_conclusion" in event_name:
                self.logger.info("强制结论节点结束")
        
        elif event_type == "on_tool_start":
            self.tool_call_count += 1
            tool_name = event.get("name", "unknown")
            tool_input = event_data.get("input", {})
            self.logger.info(f"工具调用 #{self.tool_call_count}: {tool_name}")
            tin = str(tool_input)
            if len(tin) > 400:
                tin = tin[:400] + "…"
            self.logger.info(tin)
        
        elif event_type == "on_tool_end":
            tool_name = event.get("name", "unknown")
            self.logger.info(f"工具完成: {tool_name}")
            output = event_data.get("output", {})
            content = None
            if hasattr(output, "content"):
                content = output.content
            elif isinstance(output, dict):
                content = output.get("content")
            elif isinstance(output, str):
                content = output
            if content is not None:
                cs = str(content)
                if len(cs) > 500:
                    cs = cs[:500] + "…"
                self.logger.info(cs)

        
        elif event_type == "on_chat_model_end":
            output = event_data.get("output", {})
            if hasattr(output, "content"):
                content = output.content
                if "【诊断结论】" in content:
                    self.logger.info("检测到诊断结论输出")
    
    def _handle_structured(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """返回结构化事件（为 Gradio 预留）"""
        event_type = event.get("event")
        event_name = event.get("name", "")
        event_data = event.get("data", {})
        
        structured_event = {
            "type": event_type,
            "name": event_name,
            "timestamp": datetime.now().isoformat(),
            "iteration": self.current_iteration
        }
        
        if event_type == "on_tool_start":
            structured_event.update({
                "tool_name": event_name,
                "input": event_data.get("input", {})
            })
        elif event_type == "on_tool_end":
            structured_event.update({
                "tool_name": event_name,
                "output": event_data.get("output", "")
            })
        elif event_type == "on_chat_model_stream":
            chunk = event_data.get("chunk")
            if hasattr(chunk, "content"):
                self.llm_accumulated_text += chunk.content
                structured_event.update({
                    "content": chunk.content,
                    "accumulated": self.llm_accumulated_text
                })
        elif event_type == "on_chat_model_end":
            output = event_data.get("output", {})
            if hasattr(output, "content"):
                structured_event.update({
                    "content": output.content
                })
        
        return structured_event
    
    def _log_to_file(self, event: Dict[str, Any]) -> None:
        """记录到日志文件"""
        if not self.logger:
            return
        
        event_type = event.get("event")
        event_name = event.get("name", "")
        event_data = event.get("data", {})
        
        # 详细记录所有事件到文件
        if event_type == "on_chain_start":
            self.logger.debug(f"[事件] 节点开始: {event_name}")
        elif event_type == "on_chain_end":
            self.logger.debug(f"[事件] 节点结束: {event_name}")
        elif event_type == "on_tool_start":
            tool_input = event_data.get("input", {})
            self.logger.info(f"[事件] 工具调用开始: {event_name}, 输入: {str(tool_input)[:200]}")
        elif event_type == "on_tool_end":
            output = event_data.get("output", "")
            self.logger.info(f"[事件] 工具调用完成: {event_name}, 输出长度: {len(str(output))}")
        elif event_type == "on_chat_model_stream":
            # Token 流不记录到文件（太多）
            pass
        elif event_type == "on_chat_model_end":
            output = event_data.get("output", {})
            if hasattr(output, "content"):
                content = output.content
                self.logger.debug(f"[事件] LLM完成, 输出长度: {len(content)}")
    
    def _format_tool_input(self, tool_input: Dict[str, Any]) -> str:
        """格式化工具输入"""
        if not tool_input:
            return "[dim]无输入参数[/dim]"
        
        # 如果包含 SQL，使用语法高亮
        if "sql" in tool_input:
            sql = tool_input.get("sql", "")
            syntax = Syntax(sql, "sql", theme="monokai", line_numbers=False)
            return syntax
        
        # 其他情况返回字符串
        return str(tool_input)
    
    def _note_timing(self, event: Dict[str, Any]) -> None:
        event_type = event.get("event")
        event_name = event.get("name", "")
        if event_type == "on_chat_model_start":
            self._llm_t0 = time.perf_counter()
        elif event_type == "on_chat_model_end" and self._llm_t0 is not None:
            elapsed = (time.perf_counter() - self._llm_t0) * 1000.0
            self._llm_t0 = None
            log_timing(
                "diagnosis_llm",
                elapsed,
                iter=self.current_iteration,
                node=event_name or "-",
            )
        elif event_type == "on_tool_start":
            self._tool_t0 = time.perf_counter()
            self._tool_name = event_name or "tool"
        elif event_type == "on_tool_end" and self._tool_t0 is not None:
            elapsed = (time.perf_counter() - self._tool_t0) * 1000.0
            self._tool_t0 = None
            inp = (event.get("data") or {}).get("input") or {}
            kind = sql_kind(inp.get("sql") or "") if isinstance(inp, dict) else "-"
            log_timing(
                "diagnosis_tool",
                elapsed,
                tool=self._tool_name or event_name or "-",
                kind=kind,
                n=self.tool_call_count,
            )

    def get_summary(self) -> Dict[str, Any]:
        """获取处理摘要"""
        return {
            "total_events": len(self.events_history),
            "iterations": self.current_iteration,
            "tool_calls": self.tool_call_count
        }

