import asyncpg
from dataclasses import dataclass
from typing import LiteralString, List, Dict, Any, Optional

@dataclass
class PostgresConfig:
    """Configuration for PostgreSQL connection"""
    host: str
    port: int
    user: str
    password: str
    database: str
    
class PostgresConnection:
    """Connection manager for PostgreSQL database"""
    
    def __init__(self, config: PostgresConfig):
        """Initialize PostgreSQL connection manager
        
        Args:
            config: PostgreSQL connection configuration
        """
        self.config = config
        self.pool = None
    
    async def connect(self) -> None:
        """Connect to PostgreSQL database"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database
            )
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")  # Test connection
            print("PostgreSQL connection established")
            return self.pool
        except asyncpg.PostgresError as e:  
            raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}") from e
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}") from e
    
    async def disconnect(self) -> None:
        """Disconnect from PostgreSQL database"""
        if self.pool:
            await self.pool.close()
            print("PostgreSQL connection closed")

class PostgresDriver:
    """"Driver for PostgreSQL database operations"""
    
    @dataclass
    class RowResult:
        """Simple class to match the Griptape RowResult interface."""
        cells: Dict[str, Any]
        
    def __init__(self, connection: PostgresConnection, config: PostgresConfig):
        """Initialize PostgreSQL driver
        
        Args:
            connection: PostgreSQL connection manager
        """
        self.connection = connection
        self.config = config
        
    def connect(self):
        if self.connection is not None:
            return self.connection
        elif self.config is not None:
            self.connection = PostgresConnection(self.config)
            return self.connection
        else:
            raise ValueError("No connection or config provided.")
    
    async def execute_query(
        self,
        query: LiteralString, 
        params: Optional[List[Any]] = None
    ) -> Optional[List[RowResult]]:
        """Execute a SQL query against PostgreSQL
        
        Args:
            query: SQL query to execute
            params: Query parameters
        
        Returns:
            Query results as a list of RowResult objects
        """
        try:
            if self.connection is None:
                self.connect()
                if self.connection is None:
                    raise ConnectionError("Database connection not established")
            
            pool = self.connection.pool
            if pool is None:
                pool = await self.connection.connect()
            
            if not params:
                params = []
            
            async with pool.acquire() as conn:
                result = await conn.fetch(query, *params)
                
                # Convert results to RowResult objects
                rows = [self.RowResult(dict(row)) for row in result]
                
                return rows
        except asyncpg.PostgresError as e:
            raise RuntimeError(f"PostgreSQL error: {str(e)}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {str(e)}") from e