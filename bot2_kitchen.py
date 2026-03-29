import pika
import json
import asyncio
from aiogram import Bot

from dotenv import load_dotenv
import os
import time

load_dotenv()

bot = Bot(token=os.getenv("BOT_CONSUMER_TOKEN"))

def connect_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            print("✅ Підключено до RabbitMQ")
            return connection
        except Exception as e:
            print(f"⏳ RabbitMQ не готовий, чекаю... ({e})")
            time.sleep(3)

connection = connect_rabbitmq()
channel = connection.channel()
channel.queue_declare(queue='orders')

async def process_orders():
    while True:
        orders_batch = []

        # Fetch all orders
        while True:
            method_frame, header_frame, body = channel.basic_get(queue='orders')

            if not method_frame:
                break

            order = json.loads(body)
            orders_batch.append((method_frame.delivery_tag, order))

        if orders_batch:
            for tag, order in orders_batch:
                user = order["user"]
                items = order.get("items") or [order.get("text", "невідомо")]
                items_str = ", ".join(items)

                message = f"🍳 Готово: {items_str}"

                await bot.send_message(chat_id=user, text=message)
                channel.basic_ack(tag)

        await asyncio.sleep(15)

async def main():
    await process_orders()

if __name__ == "__main__":
    asyncio.run(main())