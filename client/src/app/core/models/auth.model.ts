export type UserRole = 'admin' | 'manager' | 'viewer';
export type UserProvider = 'local' | 'google' | 'microsoft';

export interface UserDto {
  id: string;
  username: string;
  displayName: string;
  email: string;
  provider: UserProvider;
  role: UserRole;
  createdAt: string;
}

export interface ApiKeyDto {
  id: string;
  name: string;
  description: string;
  minimumLevel: number;
  keyPreview: string;
  createdBy: string;
  createdAt: string;
}

export interface CreatedApiKeyDto {
  id: string;
  name: string;
  description: string;
  minimumLevel: number;
  key: string;
  createdBy: string;
  createdAt: string;
}

export interface OAuthDomainDto {
  id: string;
  provider: 'google' | 'microsoft';
  domain: string;
  role: UserRole;
  createdAt: string;
}

export interface LoginResponseDto {
  token: string;
  expiresIn: number;
  role: UserRole;
}

export interface AuthProvidersDto {
  local: boolean;
  google: boolean;
  microsoft: boolean;
}
