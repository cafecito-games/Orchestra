import type { SVGProps } from "react";

type IconProps = SVGProps<SVGSVGElement>;

function Icon({ children, ...props }: IconProps) {
  return (
    <svg
      viewBox="0 0 16 16"
      width="16"
      height="16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...props}
    >
      {children}
    </svg>
  );
}

export function GridIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <rect x="2" y="2" width="5" height="5" rx="1" />
      <rect x="9" y="2" width="5" height="5" rx="1" />
      <rect x="2" y="9" width="5" height="5" rx="1" />
      <rect x="9" y="9" width="5" height="5" rx="1" />
    </Icon>
  );
}

export function ServerIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <rect x="2" y="2" width="12" height="5" rx="1" />
      <rect x="2" y="9" width="12" height="5" rx="1" />
      <path d="M5.5 4.5h.01M5.5 11.5h.01" />
    </Icon>
  );
}

export function ThreadIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M2.5 3.5h11v6h-6L4.5 12.5v-3h-2z" />
    </Icon>
  );
}

export function ShieldIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M8 2l5 2v4c0 3-2 4.8-5 6-3-1.2-5-3-5-6V4z" />
      <path d="M6 8l1.5 1.5L10.5 6.5" />
    </Icon>
  );
}

export function MailIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <rect x="2" y="3" width="12" height="10" rx="1.5" />
      <path d="M3 5l5 4 5-4" />
    </Icon>
  );
}

export function RefreshIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M13 8a5 5 0 1 1-1.5-3.5" />
      <path d="M13 2.5V5h-2.5" />
    </Icon>
  );
}

export function SignOutIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M6 8h7m0 0-2.5-2.5M13 8l-2.5 2.5" />
      <path d="M8 3H3.5v10H8" />
    </Icon>
  );
}

export function PlayIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <circle cx="8" cy="8" r="6" />
      <path d="M7 5.5v5l4-2.5z" />
    </Icon>
  );
}

export function CpuIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <rect x="4" y="4" width="8" height="8" rx="1.5" />
      <path d="M8 2v2M8 12v2M2 8h2M12 8h2" />
    </Icon>
  );
}

export function BackIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M10 3.5 5.5 8 10 12.5" />
    </Icon>
  );
}

export function CheckIcon(props: IconProps) {
  return (
    <Icon strokeWidth="1.6" {...props}>
      <path d="M3.5 8.5l3 3 6-7" />
    </Icon>
  );
}

export function XIcon(props: IconProps) {
  return (
    <Icon strokeWidth="1.6" {...props}>
      <path d="M4 4l8 8M12 4l-8 8" />
    </Icon>
  );
}

export function SparkIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M8 2l1.5 4.5L14 8l-4.5 1.5L8 14 6.5 9.5 2 8l4.5-1.5z" />
    </Icon>
  );
}

export function AlertIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M8 2.2 14 13H2z" />
      <path d="M8 6.4v3.2M8 11.4h.01" />
    </Icon>
  );
}

export function BellIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M8 2.2a3.8 3.8 0 0 1 3.8 3.8v2.2l1.2 1.8H3l1.2-1.8V6A3.8 3.8 0 0 1 8 2.2Z" />
      <path d="M6.6 12.2a1.6 1.6 0 0 0 2.8 0" />
    </Icon>
  );
}

export function SendIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M13.8 2.2 7.2 8.8" />
      <path d="M13.8 2.2 9.4 13.8 7.2 8.8 2.2 6.6z" />
    </Icon>
  );
}

export function ImageIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <rect x="2" y="2.5" width="12" height="11" rx="1.5" />
      <circle cx="5.5" cy="6" r="1.1" />
      <path d="M2.5 11.5 6 8.2l2.6 2.4 2-1.8 2.9 2.7" />
    </Icon>
  );
}

export function DownloadIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M8 2.5v7.5m0 0-3-3m3 3 3-3" />
      <path d="M3 12.8h10" />
    </Icon>
  );
}

export function ChevronLeftIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M10 3.5 5.5 8 10 12.5" />
    </Icon>
  );
}

export function ChevronRightIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M6 3.5 10.5 8 6 12.5" />
    </Icon>
  );
}

export function CopyIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <rect x="5.5" y="5.5" width="8" height="8" rx="1.2" />
      <path d="M10.5 5.5V3.7a1.2 1.2 0 0 0-1.2-1.2H3.7a1.2 1.2 0 0 0-1.2 1.2v5.6a1.2 1.2 0 0 0 1.2 1.2h1.8" />
    </Icon>
  );
}

export function FileIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M4 2.5h5.2L12.5 5.8v7.7a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1v-10a1 1 0 0 1 1-1Z" />
      <path d="M9 2.5V6h3.5" />
    </Icon>
  );
}

export function ReplyIcon(props: IconProps) {
  return (
    <Icon {...props}>
      <path d="M6.5 4.2 2.7 8l3.8 3.8" />
      <path d="M3 8h6.2a4.1 4.1 0 0 1 4.1 4.1v.9" />
    </Icon>
  );
}
