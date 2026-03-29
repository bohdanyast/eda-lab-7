import pika
import json
from datetime import datetime
import asyncio

from aiogram import Bot, Dispatcher
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command

from dotenv import load_dotenv
import os
import time

load_dotenv()

bot = Bot(token=os.getenv("BOT_PRODUCER_TOKEN"))
dp = Dispatcher()

def connect_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            print("Підключено до RabbitMQ")
            return connection
        except Exception as e:
            print(f"RabbitMQ не готовий! ({e})")
            time.sleep(3)


def send_to_queue(order: dict):
    # Each time new conn
    connection = connect_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue='orders')
    
    channel.basic_publish(
        exchange='',
        routing_key='orders',
        body=json.dumps(order)
    )
    
    connection.close()

# TG Buttons
keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🍕 Піца"), KeyboardButton(text="🍔 Бургер")],
        [KeyboardButton(text="🥤 Напій")],
        [KeyboardButton(text="🧾 Моє замовлення"), KeyboardButton(text="✅ Оформити")],
        [KeyboardButton(text="❌ Скасувати")]
    ],
    resize_keyboard=True
)

# Storing user orders
user_orders = {}

@dp.message(Command("start"))
async def start(message: Message):
    user_orders[message.chat.id] = []
    await message.answer("🍽️ Обери замовлення:", reply_markup=keyboard)

@dp.message()
async def handle_buttons(message: Message):
    user_id = message.chat.id
    text = message.text

    if user_id not in user_orders:
        user_orders[user_id] = []

    # Cancel order
    if text == "❌ Скасувати":
        user_orders[user_id] = []
        await message.answer("❌ Замовлення очищено")
        return

    # Add food to order
    if text in ["🍕 Піца", "🍔 Бургер", "🥤 Напій"]:
        user_orders[user_id].append(text)
        await message.answer(f"➕ Додано: {text}")
        return

    # Look through order
    if text == "🧾 Моє замовлення":
        if not user_orders[user_id]:
            await message.answer("📭 Замовлення порожнє")
        else:
            order_list = "\n".join(user_orders[user_id])
            await message.answer(f"🧾 Твоє замовлення:\n{order_list}")
        return

    # Send to consumer
    if text == "✅ Оформити":
        if not user_orders[user_id]:
            await message.answer("⚠️ Немає що оформлювати")
            return

        order = {
            "user": user_id,
            "items": user_orders[user_id],
            "time": datetime.now().strftime("%H:%M:%S")
        }

        send_to_queue(order)

        await message.answer("✅ Замовлення відправлено на кухню")
        user_orders[user_id] = []

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())