"""
Глобальное состояние приложения.

Coordinator создаётся один раз при старте и живёт всё время работы сервера.
Все роутеры импортируют его отсюда — никаких синглтонов через модули.
"""

from app.core.coordinator import Coordinator

coordinator = Coordinator()
