import logging

import jwt
from fastapi import HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from hmpps_person_match.dependencies.auth.jwks import JWKS
from hmpps_person_match.domain.constants.error_messages import ErrorMessages
from hmpps_person_match.utils.environment import EnvVars, get_env_var


class JWTBearer(HTTPBearer):
    """
    JWT Authentication dependency class
    Validates JWT tokens and raises an HTTPException if invalid or expired
    """

    def __init__(self, required_roles: list | None = None) -> None:
        super().__init__()
        if required_roles is None:
            required_roles = []
        self.required_roles = required_roles
        self.logger = logging.getLogger("hmpps-person-match-logger")

    async def __call__(self, request: Request) -> None:
        """
        Class initialization function
        Verify provided auth token is valid
        """
        credentials: HTTPAuthorizationCredentials = await super().__call__(request)
        if credentials:
            await self.verify_jwt(credentials.credentials)
        else:
            raise HTTPException(status_code=401, detail=ErrorMessages.INVALID_AUTH_HEADER)

    async def verify_jwt(self, token: str) -> None:
        """
        Verify JWT validity
        """
        try:
            # Fetch the public key based on the 'kid' in the JWT header
            public_key = await JWKS().get_public_key_from_jwt(token)
            pem_key = public_key.as_pem(is_private=False)

            issuer = f"{get_env_var(EnvVars.OAUTH_ISSUER_URL_KEY)}/auth/issuer"

            # Decode and validate the JWT with the public key
            payload = jwt.decode(
                token,
                pem_key,
                algorithms=JWKS.ALGORITHMS,
                issuer=issuer,
            )

            # Check if the required role is in the JWT's roles claim
            user_roles = payload.get("authorities", [])
            if not set(self.required_roles).intersection(user_roles):
                raise HTTPException(status_code=403, detail=ErrorMessages.FORBIDDEN)

        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError, ValueError) as error:
            raise HTTPException(status_code=401, detail=ErrorMessages.INVALID_OR_EXPIRED_TOKEN) from error
