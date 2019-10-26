import asyncio
import sys

from .main import main


rv = asyncio.run(main())
sys.exit(rv)
