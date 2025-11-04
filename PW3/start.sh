#!/bin/bash

echo "Генерация тестовых данных..."
python3 data_generator.py

echo "Запуск Docker Compose..."
sudo docker compose up -d

echo "Сервисы запущены!"
echo "Jupyter Lab: http://localhost:8888"
echo "Для остановки: sudo docker compose down"
