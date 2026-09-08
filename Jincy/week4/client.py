import asyncio

from secure_mesh import submit_task


def add(left, right):
    return left + right


async def main():
    result = await submit_task(
        "127.0.0.1",
        8000,
        "node8002",
        add,
        (10, 20),
    )
    print(f"Signed task result: {result}")


if __name__ == "__main__":
    asyncio.run(main())