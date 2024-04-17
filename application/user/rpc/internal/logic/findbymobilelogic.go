package logic

import (
	"context"
	"fmt"

	"beyond/application/user/rpc/internal/svc"
	"beyond/application/user/rpc/service"

	"github.com/zeromicro/go-zero/core/logx"
)

type FindByMobileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFindByMobileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FindByMobileLogic {
	return &FindByMobileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *FindByMobileLogic) FindByMobile(in *service.FindByMobileRequest) (*service.FindByMobileResponse, error) {
	// todo: add your logic here and delete this line
	fmt.Println("rpc mobile:", in.Mobile)
	user, err := l.svcCtx.UserModel.FindByMobile(l.ctx, in.Mobile)
	if err != nil {
		logx.Errorf("FindByMobile mobile: %s error: %v", in.Mobile, err)
		return nil, err
	}
	if user == nil {
		return &service.FindByMobileResponse{}, nil
	}

	return &service.FindByMobileResponse{
		UserId:   user.Id,
		Username: user.Username,
		Avatar:   user.Avatar,
	}, nil

	//return &service.FindByMobileResponse{}, nil
}
