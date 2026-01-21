from server.api import api_client


class RFactor:

    @staticmethod
    async def update_r_factor():

        await api_client.get_json(
            "https://market-bot-api-five.vercel.app/update_data",
            params={
                "fetch_option_chain": 0,
                "password": "valid123",
            },
            headers={
                "Content-Type": "application/json",
            },
        )


# async def main():
# async with APIClient() as client:
# await worker_loop(client)


# if __name__ == "__main__":
#     asyncio.run(main())
