import gradio as gr
from chatbot_with_graph import chat_response
import traceback

with gr.Blocks(title="Local Customer Chatbot") as demo:
    chatbot = gr.Chatbot()
    msg = gr.Textbox(label="Your message")
    clear = gr.Button("Clear")

    def respond(message, chat_history):
        chat_history = chat_history or []
        try:
            bot_reply = chat_response(message)
            if not bot_reply:
                bot_reply = "[No answer returned]"
        except Exception as e:
            error_trace = traceback.format_exc()
            print("[Gradio Error]", error_trace)
            bot_reply = f"[Error] {str(e)}"
        chat_history.append((message, bot_reply))
        return "", chat_history

    msg.submit(respond, [msg, chatbot], [msg, chatbot])
    clear.click(lambda: ("", []), None, [msg, chatbot], queue=False)

if __name__ == "__main__":
    demo.launch()
