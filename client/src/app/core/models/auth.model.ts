export interface UserDto {
  id: string;
  username: string;
  role: 'admin' | 'manager';
  createdAt: string;
}

export interface ApiKeyDto {
  id: string;
  name: string;
  keyPreview: string;
  createdBy: string;
  createdAt: string;
}

export interface CreatedApiKeyDto {
  id: string;
  name: string;
  key: string;
  createdBy: string;
  createdAt: string;
}

export interface LoginResponseDto {
  token: string;
  expiresIn: number;
}
