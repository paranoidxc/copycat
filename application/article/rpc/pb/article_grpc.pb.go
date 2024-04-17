// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.21.9
// source: article.proto

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	Article_Publish_FullMethodName  = "/pb.Article/Publish"
	Article_Articles_FullMethodName = "/pb.Article/Articles"
)

// ArticleClient is the client API for Article service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ArticleClient interface {
	Publish(ctx context.Context, in *PublishRequest, opts ...grpc.CallOption) (*PublishResponse, error)
	Articles(ctx context.Context, in *ArticlesRequest, opts ...grpc.CallOption) (*ArticlesResponse, error)
}

type articleClient struct {
	cc grpc.ClientConnInterface
}

func NewArticleClient(cc grpc.ClientConnInterface) ArticleClient {
	return &articleClient{cc}
}

func (c *articleClient) Publish(ctx context.Context, in *PublishRequest, opts ...grpc.CallOption) (*PublishResponse, error) {
	out := new(PublishResponse)
	err := c.cc.Invoke(ctx, Article_Publish_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *articleClient) Articles(ctx context.Context, in *ArticlesRequest, opts ...grpc.CallOption) (*ArticlesResponse, error) {
	out := new(ArticlesResponse)
	err := c.cc.Invoke(ctx, Article_Articles_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ArticleServer is the server API for Article service.
// All implementations must embed UnimplementedArticleServer
// for forward compatibility
type ArticleServer interface {
	Publish(context.Context, *PublishRequest) (*PublishResponse, error)
	Articles(context.Context, *ArticlesRequest) (*ArticlesResponse, error)
	mustEmbedUnimplementedArticleServer()
}

// UnimplementedArticleServer must be embedded to have forward compatible implementations.
type UnimplementedArticleServer struct {
}

func (UnimplementedArticleServer) Publish(context.Context, *PublishRequest) (*PublishResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Publish not implemented")
}
func (UnimplementedArticleServer) Articles(context.Context, *ArticlesRequest) (*ArticlesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Articles not implemented")
}
func (UnimplementedArticleServer) mustEmbedUnimplementedArticleServer() {}

// UnsafeArticleServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ArticleServer will
// result in compilation errors.
type UnsafeArticleServer interface {
	mustEmbedUnimplementedArticleServer()
}

func RegisterArticleServer(s grpc.ServiceRegistrar, srv ArticleServer) {
	s.RegisterService(&Article_ServiceDesc, srv)
}

func _Article_Publish_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PublishRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ArticleServer).Publish(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Article_Publish_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ArticleServer).Publish(ctx, req.(*PublishRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Article_Articles_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ArticlesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ArticleServer).Articles(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Article_Articles_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ArticleServer).Articles(ctx, req.(*ArticlesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Article_ServiceDesc is the grpc.ServiceDesc for Article service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Article_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "pb.Article",
	HandlerType: (*ArticleServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Publish",
			Handler:    _Article_Publish_Handler,
		},
		{
			MethodName: "Articles",
			Handler:    _Article_Articles_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "article.proto",
}
