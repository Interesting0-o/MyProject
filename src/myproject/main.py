import asyncio

from myproject.config import get_config


async def get_user_input(s: str):
    num = await asyncio.to_thread(input, f"{s}\n")
    try:
        num = int(num)
        await asyncio.sleep(2)
    except Exception as e:
        print(e)
        return
    print(num * num, "\n")


async def func(s: str):
    print(f"{s} is Start")
    await asyncio.sleep(1)
    print(f"{s} is End")


async def togther():
    await asyncio.gather(func("A"), func("B"), get_user_input("1"), get_user_input("2"))


def main():
    asyncio.run(togther())
